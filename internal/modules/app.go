// Package modules is the sole composition root. Constructor injection replaces
package modules

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"jungle/internal/adapters/auth"
	"jungle/internal/adapters/httpapi"
	"jungle/internal/adapters/observability"
	"jungle/internal/adapters/postgres"
	"jungle/internal/adapters/sqs"
	"jungle/internal/platform/config"
	"jungle/internal/workers"
)

var ConfigModule = fx.Module("config", fx.Provide(config.Load))
var ObservabilityModule = fx.Module("observability", fx.Provide(func() *slog.Logger { return slog.New(slog.NewJSONHandler(os.Stdout, nil)) }, observability.New))
var PersistenceModule = fx.Module("persistence", fx.Provide(newPool, postgres.New))
var AuthModule = fx.Module("auth", fx.Provide(func(c config.Config) (*auth.Verifier, error) {
	return auth.New(auth.Config{Issuer: c.OIDCIssuer, FetchIssuer: c.OIDCFetchIssuer, Audience: c.OIDCAudience, AllowInsecureHTTP: c.OIDCAllowHTTP}, nil)
}))
var ApplicationModule = fx.Module("application", fx.Provide(func(s *postgres.Store, m *observability.Metrics, l *slog.Logger) *observability.Service {
	return &observability.Service{Wallets: s, Wagers: s, Metrics: m, Logger: l}
}))
var MessagingModule = fx.Module("messaging", fx.Provide(sqs.New))
var WorkerModule = fx.Module("workers", fx.Provide(func(s *postgres.Store, b *sqs.Client, c config.Config, l *slog.Logger, m *observability.Metrics) *workers.Engine {
	return workers.New(s, b, c, l, m)
}))
var HTTPModule = fx.Module("http", fx.Provide(newRuntime), fx.Invoke(func(*Runtime) {}))
var App = fx.Module("jungle", ConfigModule, ObservabilityModule, PersistenceModule, AuthModule, ApplicationModule, MessagingModule, WorkerModule, HTTPModule)

func New(options ...fx.Option) *fx.App {
	base := []fx.Option{App, fx.StartTimeout(45 * time.Second), fx.StopTimeout(50 * time.Second), fx.WithLogger(func(l *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: l} }), fx.Invoke(reportProcs)}
	return fx.New(append(base, options...)...)
}

// reportProcs records what the runtime decided about parallelism. Since Go 1.25
// the runtime already derives GOMAXPROCS from the cgroup CPU limit, so no
// automaxprocs dependency is needed; what an operator still cannot see from
// outside is the gap between the node's cores and the container's quota, which
// is where CFS throttling hides. Logging both makes that gap visible.
func reportProcs(l *slog.Logger) {
	l.Info("runtime parallelism", "gomaxprocs", runtime.GOMAXPROCS(0), "visible_cpus", runtime.NumCPU())
}

// version is overridable at link time (-ldflags "-X jungle/internal/modules.version=...").
var version = "dev"

func newPool(c config.Config, lc fx.Lifecycle, metrics *observability.Metrics) (*pgxpool.Pool, error) {
	options, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("database configuration: %w", err)
	}
	options.MaxConns = int32(c.MaxConns)
	options.MinConns = 2
	// A burst grows the pool to MaxConns; pgx would otherwise hold those
	// connections for half an hour, so a finished load run keeps starving every
	// other client of the same PostgreSQL. Release idle capacity promptly.
	options.MaxConnIdleTime = 2 * time.Minute
	options.ConnConfig.RuntimeParams["application_name"] = c.InstanceID
	options.ConnConfig.Tracer = metrics.SQLTracer()
	pool, err := pgxpool.NewWithConfig(context.Background(), options)
	if err != nil {
		return nil, err
	}
	// Registered here rather than in New(): the collectors read live pool state,
	// so they can only exist once the pool does.
	metrics.PoolCollector(pool.Stat)
	metrics.BuildInfo(version, c.InstanceID)
	lc.Append(fx.Hook{OnStart: pool.Ping, OnStop: func(context.Context) error { pool.Close(); return nil }})
	return pool, nil
}

type Runtime struct {
	ready     atomic.Bool
	Server    *http.Server
	workers   *workers.Engine
	logger    *slog.Logger
	serveDone chan struct{}
}

func newRuntime(c config.Config, pool *pgxpool.Pool, identity *auth.Verifier, broker *sqs.Client, service *observability.Service, engine *workers.Engine, metrics *observability.Metrics, logger *slog.Logger, lc fx.Lifecycle, shutdown fx.Shutdowner) (*Runtime, error) {
	r := &Runtime{workers: engine, logger: logger, serveDone: make(chan struct{})}
	handler, err := httpapi.New(httpapi.Dependencies{Wallets: service, Wagers: service, Auth: identity, Logger: logger, Metrics: metrics.Handler(), Observe: metrics.Observe, BodyLimit: 128 * 1024, RequestTimeout: c.ProcessTimeout, Ready: func(ctx context.Context) error {
		if !r.ready.Load() {
			return errors.New("draining")
		}
		if err := pool.Ping(ctx); err != nil {
			return err
		}
		return broker.Check(ctx)
	}})
	if err != nil {
		return nil, err
	}
	r.Server = &http.Server{Addr: c.HTTPAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: c.ProcessTimeout + 5*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 * 1024}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		if err := identity.Check(ctx); err != nil {
			return err
		}
		if err := broker.Start(ctx); err != nil {
			return err
		}
		listener, err := net.Listen("tcp", c.HTTPAddr)
		if err != nil {
			return err
		}
		engine.Start()
		r.ready.Store(true)
		go func() {
			defer close(r.serveDone)
			if err := r.Server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("HTTP server stopped unexpectedly")
				_ = shutdown.Shutdown(fx.ExitCode(1))
			}
		}()
		logger.Info("service ready", "instanceId", c.InstanceID, "address", listener.Addr().String())
		return nil
	}, OnStop: func(parent context.Context) error {
		ctx, cancel := context.WithTimeout(parent, c.ShutdownTimeout)
		defer cancel()
		r.ready.Store(false)
		engine.StopIntake()
		// Do not cancel request contexts before Shutdown drains accepted work.
		httpErr := r.Server.Shutdown(ctx)
		if httpErr != nil {
			_ = r.Server.Close()
		}
		workerErr := engine.Stop(ctx)
		logger.Info("service drained", "instanceId", c.InstanceID)
		return errors.Join(httpErr, workerErr)
	}})
	return r, nil
}
