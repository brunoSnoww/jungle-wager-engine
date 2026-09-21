// Package httpapi adapts strict HTTP contracts to transport-independent use cases.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"jungle/internal/adapters/auth"
	"jungle/internal/application"
	"jungle/internal/domain"
)

type Authenticator interface {
	Validate(context.Context, string) (auth.Principal, error)
}
type Dependencies struct {
	Wallets        application.Wallets
	Wagers         application.Wagers
	Auth           Authenticator
	Ready          func(context.Context) error
	Metrics        http.Handler
	Logger         *slog.Logger
	Observe        func(method, route string, status int, elapsed time.Duration)
	BodyLimit      int64
	RequestTimeout time.Duration
}

type adapter struct{ dependencies Dependencies }
type contextKey uint8

const (
	principalKey contextKey = iota
	correlationKey
)

func New(dependencies Dependencies) (http.Handler, error) {
	if dependencies.Wallets == nil || dependencies.Wagers == nil || dependencies.Auth == nil || dependencies.Ready == nil {
		return nil, errors.New("HTTP requires wallet/wager use cases, authentication and readiness")
	}
	if dependencies.BodyLimit == 0 {
		dependencies.BodyLimit = 64 << 10
	}
	if dependencies.BodyLimit < 1 {
		return nil, errors.New("HTTP body limit must be positive")
	}
	if dependencies.RequestTimeout == 0 {
		dependencies.RequestTimeout = 30 * time.Second
	}
	if dependencies.RequestTimeout < 0 {
		return nil, errors.New("HTTP request timeout must be positive")
	}
	if dependencies.Logger == nil {
		dependencies.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	a := &adapter{dependencies: dependencies}
	router := chi.NewRouter()
	router.Use(a.requestContext)
	router.Get("/health/live", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
	})
	router.Get("/health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := dependencies.Ready(ctx); err != nil {
			a.failure(w, r, http.StatusServiceUnavailable, "NOT_READY", "dependencies unavailable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	if dependencies.Metrics != nil {
		router.Handle("/metrics", dependencies.Metrics)
	}
	for _, prefix := range []string{"", "/v1"} {
		router.Route(prefix+"/wallets", func(r chi.Router) {
			r.With(a.authorize("wallet:write", false)).Post("/", a.createWallet)
			r.With(a.authorize("wallet:read", false)).Get("/{walletId}", a.getWallet)
			r.With(a.authorize("wallet:read", false)).Get("/{walletId}/ledger", a.ledger)
			r.With(a.authorize("wallet:reconcile", false)).Post("/{walletId}/reconciliation", a.reconcile)
		})
		router.With(a.authorize("wager:write", true)).Post(prefix+"/wagering/transactions", a.processWager)
		router.With(a.authorize("wager:read", true)).Get(prefix+"/wagering/transactions/{transactionId}", a.getTransaction)
		router.With(a.authorize("wager:read", true)).Get(prefix+"/providers/{providerId}/wagering/transactions/{externalTransactionId}", a.getExternal)
	}
	return router, nil
}

func (a *adapter) authorize(scope string, providerRequired bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			parts := strings.Fields(r.Header.Get("Authorization"))
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				w.Header().Set("WWW-Authenticate", "Bearer")
				a.failure(w, r, 401, "UNAUTHENTICATED", "valid bearer token required")
				return
			}
			principal, err := a.dependencies.Auth.Validate(r.Context(), parts[1])
			if err != nil {
				w.Header().Set("WWW-Authenticate", "Bearer")
				a.failure(w, r, 401, "UNAUTHENTICATED", "valid bearer token required")
				return
			}
			if !principal.HasScope(scope) || (providerRequired && principal.ProviderID == "") || (!providerRequired && principal.ProviderID != "") {
				a.failure(w, r, 403, "FORBIDDEN", "operation not permitted")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey, principal)))
		})
	}
}

func Principal(ctx context.Context) auth.Principal {
	p, _ := ctx.Value(principalKey).(auth.Principal)
	return p
}
func CorrelationID(ctx context.Context) string {
	value, _ := ctx.Value(correlationKey).(string)
	return value
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}
func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (a *adapter) requestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		correlation := r.Header.Get("X-Correlation-ID")
		if correlation != "" && !validCorrelation(correlation) {
			a.failure(w, r, 400, "INVALID_CORRELATION_ID", "invalid correlation header")
			return
		}
		if correlation == "" {
			var id [16]byte
			if _, err := rand.Read(id[:]); err != nil {
				a.failure(w, r, 503, "UNAVAILABLE", "service unavailable")
				return
			}
			correlation = hex.EncodeToString(id[:])
		}
		ctx, cancel := context.WithTimeout(context.WithValue(r.Context(), correlationKey, correlation), a.dependencies.RequestTimeout)
		defer cancel()
		r = r.WithContext(ctx)
		w.Header().Set("X-Correlation-ID", correlation)
		wrapped := &responseWriter{ResponseWriter: w}
		defer func() {
			if recovered := recover(); recovered != nil {
				a.dependencies.Logger.ErrorContext(ctx, "HTTP panic", "correlation_id", correlation)
				if wrapped.status == 0 {
					a.failure(wrapped, r, 500, "INTERNAL_ERROR", "request failed")
				}
			}
			status := wrapped.status
			if status == 0 {
				status = 200
			}
			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unmatched"
			}
			if a.dependencies.Observe != nil {
				method := r.Method
				switch method {
				case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead, http.MethodOptions, http.MethodConnect, http.MethodTrace:
				default:
					method = "OTHER"
				}
				a.dependencies.Observe(method, route, status, time.Since(start))
			}
			a.dependencies.Logger.InfoContext(ctx, "HTTP request", "correlation_id", correlation, "method", r.Method, "route", route, "status", status, "duration_ms", time.Since(start).Milliseconds())
		}()
		next.ServeHTTP(wrapped, r)
	})
}

func validCorrelation(value string) bool {
	if len(value) > 128 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.:", c)) {
			return false
		}
	}
	return true
}

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
type walletDTO struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}
type wagerDTO struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId,omitempty"`
}

func (a *adapter) decode(w http.ResponseWriter, r *http.Request, destination any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		a.failure(w, r, 415, "INVALID_CONTENT_TYPE", "application/json required")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, a.dependencies.BodyLimit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		a.decodeError(w, r, err)
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			err = errors.New("trailing JSON")
		}
		a.decodeError(w, r, err)
		return false
	}
	return true
}
func (a *adapter) decodeError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		a.failure(w, r, 413, "BODY_TOO_LARGE", "request body exceeds limit")
		return
	}
	a.failure(w, r, 400, "INVALID_JSON", "one valid JSON document with known fields required")
}

func (a *adapter) createWallet(w http.ResponseWriter, r *http.Request) {
	var body walletDTO
	if !a.decode(w, r, &body) {
		return
	}
	player, err := domain.ParseID(body.PlayerID)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	money, err := domain.ParseMoney(body.InitialBalance.Amount, body.InitialBalance.Currency)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	result, err := a.dependencies.Wallets.CreateWallet(r.Context(), application.CreateWalletCommand{PlayerID: player.String(), InitialBalance: money, CorrelationID: CorrelationID(r.Context())})
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (a *adapter) processWager(w http.ResponseWriter, r *http.Request) {
	var body wagerDTO
	if !a.decode(w, r, &body) {
		return
	}
	provider := Principal(r.Context()).ProviderID
	if body.ProviderID != provider {
		a.failure(w, r, 403, "FORBIDDEN", "provider does not match authenticated principal")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if strings.TrimSpace(key) == "" || len(key) > 256 {
		a.failure(w, r, 400, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key required, maximum 256 bytes")
		return
	}
	wallet, err := domain.ParseID(body.WalletID)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	player, err := domain.ParseID(body.PlayerID)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	money, err := domain.ParseMoney(body.Money.Amount, body.Money.Currency)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	command := application.Command{ProviderID: provider, ExternalTransactionID: body.ExternalTransactionID, IdempotencyKey: key, WalletID: wallet.String(), PlayerID: player.String(), RoundID: body.RoundID, GameID: body.GameID, Kind: body.Kind, Money: money, ReferenceExternalTransactionID: body.ReferenceExternalTransactionID, CorrelationID: CorrelationID(r.Context())}
	result, err := a.dependencies.Wagers.ProcessWager(r.Context(), command, nil)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	status := http.StatusCreated
	if result.IdempotentReplay {
		status = http.StatusOK
	} else if result.Status == "PENDING_REFERENCE" || result.Status == "PENDING" {
		status = http.StatusAccepted
	} else if result.Status == "REJECTED" {
		status = http.StatusUnprocessableEntity
	} else if result.Status == "FAILED" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, result)
}

func (a *adapter) walletID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id, err := domain.ParseID(chi.URLParam(r, "walletId"))
	if err != nil {
		a.applicationError(w, r, err)
		return "", false
	}
	return id.String(), true
}
func (a *adapter) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletID(w, r)
	if !ok {
		return
	}
	result, err := a.dependencies.Wallets.GetWallet(r.Context(), id)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}
func (a *adapter) ledger(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletID(w, r)
	if !ok {
		return
	}
	limit := 50
	if value := r.URL.Query().Get("limit"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 100 {
			a.failure(w, r, 400, "INVALID_LIMIT", "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	cursor := r.URL.Query().Get("cursor")
	if len(cursor) > 1024 {
		a.failure(w, r, 400, "INVALID_CURSOR", "invalid cursor")
		return
	}
	result, err := a.dependencies.Wallets.Ledger(r.Context(), id, cursor, limit)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}
func (a *adapter) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := a.walletID(w, r)
	if !ok {
		return
	}
	result, err := a.dependencies.Wallets.Reconcile(r.Context(), id)
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}
func (a *adapter) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, err := domain.ParseID(chi.URLParam(r, "transactionId"))
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	result, err := a.dependencies.Wagers.GetTransaction(r.Context(), Principal(r.Context()).ProviderID, id.String())
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}
func (a *adapter) getExternal(w http.ResponseWriter, r *http.Request) {
	provider := Principal(r.Context()).ProviderID
	if chi.URLParam(r, "providerId") != provider {
		a.failure(w, r, 403, "FORBIDDEN", "provider does not match authenticated principal")
		return
	}
	result, err := a.dependencies.Wagers.GetExternalTransaction(r.Context(), provider, chi.URLParam(r, "externalTransactionId"))
	if err != nil {
		a.applicationError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}

func (a *adapter) applicationError(w http.ResponseWriter, r *http.Request, err error) {
	code := application.Code(err)
	status, message := http.StatusBadRequest, "invalid request"
	switch code {
	case "REFERENCE_REQUIRED", "PLAYER_MISMATCH":
		status, message = 400, "request does not match the required transaction identity"
	case "NOT_FOUND":
		status, message = 404, "resource not found"
	case "WALLET_ALREADY_EXISTS", "IDEMPOTENCY_KEY_REUSED", "EXTERNAL_TRANSACTION_ID_REUSED":
		status, message = 409, "operation conflicts with persisted state"
	case "FORBIDDEN":
		status, message = 403, "operation not permitted"
	case "UNAVAILABLE":
		status, message = 503, "service temporarily unavailable"
	case "INSUFFICIENT_FUNDS", "INSUFFICIENT_FUNDS_FOR_REVERSAL", "CURRENCY_MISMATCH", "REFERENCE_NOT_FOUND", "REFERENCE_NOT_PROCESSED", "REFERENCE_MISMATCH", "REFERENCE_ALREADY_REVERSED", "INVALID_AMOUNT_FOR_KIND":
		status, message = 422, "operation rejected by business rule"
	default:
		if !strings.HasPrefix(code, "INVALID_") && code != "OVERFLOW" {
			status, message, code = 503, "service temporarily unavailable", "UNAVAILABLE"
		}
	}
	if status >= 500 {
		a.dependencies.Logger.ErrorContext(r.Context(), "HTTP use case unavailable", "correlation_id", CorrelationID(r.Context()), "code", code)
	}
	a.failure(w, r, status, code, message)
}
func (a *adapter) failure(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	writeJSON(w, status, struct {
		Code          string `json:"code"`
		Message       string `json:"message"`
		CorrelationID string `json:"correlationId"`
	}{code, message, CorrelationID(r.Context())})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		http.Error(w, "response unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}
