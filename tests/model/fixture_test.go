//go:build integration

// Package model_test drives the real store with randomly generated, deliberately
// adversarial operation sequences and then audits what survived.
//
// The hand-written suites prove the cases someone thought of. This one generates
// combinations nobody picked: a reversal of a reversal, a rollback that arrives
// before its reference, the same idempotency key with a different payload, two
// reversals racing for one reference. The oracle is not a second implementation
// of the ledger -- it is the set of properties that must hold under every
// interleaving, plus the SQL auditor for what the client cannot see.
package model_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"jungle/internal/adapters/postgres"
	"jungle/internal/application"
	"jungle/internal/domain"
)

var (
	gooseOnce sync.Once
	gooseMu   sync.Mutex
)

type fixture struct {
	pool  *pgxpool.Pool
	store *postgres.Store
}

func openPool(t *testing.T, dsn string, max int32) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Capped on purpose: this test opens many connections by design, and an
	// uncapped pool per host core would exhaust the server instead of the code.
	cfg.MaxConns = max
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func database(t *testing.T, poolSize int32) fixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL required: real PostgreSQL 18 conformance run")
	}
	admin := openPool(t, dsn, 2)
	var salt [4]byte
	if _, err := rand.Read(salt[:]); err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("jungle_model_%d_%x", time.Now().UnixNano(), salt)
	if _, err := admin.Exec(context.Background(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	scoped := u.String()

	db, err := sql.Open("pgx", scoped)
	if err != nil {
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
	// goose holds its dialect in a package variable; set it once, migrate under
	// a lock, because several fixtures may be building schemas at the same time.
	gooseMu.Lock()
	gooseOnce.Do(func() { err = goose.SetDialect("postgres") })
	if err == nil {
		err = goose.UpContext(context.Background(), db, filepath.Join(root, "migrations"))
	}
	gooseMu.Unlock()
	_ = db.Close()
	if err != nil {
		t.Fatal(err)
	}
	pool := openPool(t, scoped, poolSize)
	return fixture{pool: pool, store: postgres.New(pool)}
}

func money(t *testing.T, value string) domain.Money {
	t.Helper()
	m, err := domain.ParseMoney(value, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func openWallet(t *testing.T, f fixture, amount string) application.WalletView {
	t.Helper()
	var pid string
	if err := f.pool.QueryRow(context.Background(), "SELECT uuidv7()::text").Scan(&pid); err != nil {
		t.Fatal(err)
	}
	w, err := f.store.CreateWallet(context.Background(), application.CreateWalletCommand{
		PlayerID: pid, InitialBalance: money(t, amount), CorrelationID: "model"})
	if err != nil {
		t.Fatal(err)
	}
	return w
}
