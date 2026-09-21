package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jungle/internal/adapters/auth"
	"jungle/internal/application"
	"jungle/internal/domain"
)

type fakeAuth struct{}

func (fakeAuth) Validate(_ context.Context, raw string) (auth.Principal, error) {
	switch raw {
	case "internal":
		return auth.Principal{Subject: "internal", Scopes: map[string]bool{"wallet:write": true, "wallet:read": true, "wallet:reconcile": true}}, nil
	case "provider-a":
		return auth.Principal{Subject: "a", ProviderID: "provider-a", Scopes: map[string]bool{"wager:read": true, "wager:write": true}}, nil
	case "read-only":
		return auth.Principal{Subject: "a", ProviderID: "provider-a", Scopes: map[string]bool{"wager:read": true}}, nil
	default:
		return auth.Principal{}, auth.ErrInvalidToken
	}
}

type fakeService struct {
	calls           int
	command         application.Command
	err             error
	result          application.Result
	queriedProvider string
}

func (f *fakeService) CreateWallet(_ context.Context, c application.CreateWalletCommand) (application.WalletView, error) {
	f.calls++
	return application.WalletView{ID: testWallet, PlayerID: c.PlayerID, Balance: c.InitialBalance, Version: 1}, f.err
}
func (f *fakeService) GetWallet(context.Context, string) (application.WalletView, error) {
	f.calls++
	m, _ := domain.Zero("BRL")
	return application.WalletView{ID: testWallet, Balance: m}, f.err
}
func (f *fakeService) Ledger(context.Context, string, string, int) (application.LedgerPage, error) {
	f.calls++
	return application.LedgerPage{Entries: []application.LedgerEntry{}}, f.err
}
func (f *fakeService) Reconcile(context.Context, string) (application.Reconciliation, error) {
	f.calls++
	m, _ := domain.Zero("BRL")
	return application.Reconciliation{WalletID: testWallet, StoredBalance: m, CalculatedBalance: m, Difference: m, Consistent: true}, f.err
}
func (f *fakeService) ProcessWager(_ context.Context, c application.Command, _ *application.InboxMessage) (application.Result, error) {
	f.calls++
	f.command = c
	return f.result, f.err
}
func (f *fakeService) GetTransaction(_ context.Context, p, id string) (application.Result, error) {
	f.calls++
	f.queriedProvider = p
	return f.result, f.err
}
func (f *fakeService) GetExternalTransaction(_ context.Context, p, id string) (application.Result, error) {
	f.calls++
	f.queriedProvider = p
	return f.result, f.err
}

const testWallet = "0192f291-27dd-7d3f-8071-5f8685deef37"
const testPlayer = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
const validBody = `{"providerId":"provider-a","externalTransactionId":"bet-1","playerId":"` + testPlayer + `","walletId":"` + testWallet + `","roundId":"round-1","gameId":"game-1","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}`

func testHandler(t *testing.T, f *fakeService, limit int64) http.Handler {
	t.Helper()
	if !f.result.Balance.Valid() {
		f.result.Balance, _ = domain.ParseMoney("75.00", "BRL")
	}
	if f.result.Status == "" {
		f.result.Status = "PROCESSED"
	}
	h, err := New(Dependencies{Wallets: f, Wagers: f, Auth: fakeAuth{}, Ready: func(context.Context) error { return nil }, BodyLimit: limit})
	if err != nil {
		t.Fatal(err)
	}
	return h
}
func TestStrictContractsAndAuthorization(t *testing.T) {
	for _, test := range []struct {
		name, body, token, providerPath, key string
		status                               int
		limit                                int64
	}{
		{name: "valid", body: validBody, token: "provider-a", key: "original-key", status: 201},
		{name: "no token", body: validBody, key: "k", status: 401},
		{name: "invalid token", body: validBody, token: "invalid", key: "k", status: 401},
		{name: "scope", body: validBody, token: "read-only", key: "k", status: 403},
		{name: "provider mismatch", body: strings.Replace(validBody, "provider-a", "provider-b", 1), token: "provider-a", key: "k", status: 403},
		{name: "unknown field", body: strings.Replace(validBody, `"kind":"BET"`, `"unknown":true,"kind":"BET"`, 1), token: "provider-a", key: "k", status: 400},
		{name: "nested unknown field", body: strings.Replace(validBody, `"amount":"25.00"`, `"amount":"25.00","unknown":true`, 1), token: "provider-a", key: "k", status: 400},
		{name: "trailing document", body: validBody + ` {}`, token: "provider-a", key: "k", status: 400},
		{name: "numeric amount", body: strings.Replace(validBody, `"25.00"`, `25.00`, 1), token: "provider-a", key: "k", status: 400},
		{name: "negative amount", body: strings.Replace(validBody, `"25.00"`, `"-25.00"`, 1), token: "provider-a", key: "k", status: 400},
		{name: "too large", body: validBody, token: "provider-a", key: "k", status: 413, limit: 20},
		{name: "missing key", body: validBody, token: "provider-a", status: 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := &fakeService{}
			h := testHandler(t, f, test.limit)
			request := httptest.NewRequest(http.MethodPost, "/wagering/transactions", strings.NewReader(test.body))
			request.Header.Set("Content-Type", "application/json")
			if test.token != "" {
				request.Header.Set("Authorization", "Bearer "+test.token)
			}
			request.Header.Set("Idempotency-Key", test.key)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d wanted=%d body=%s", response.Code, test.status, response.Body.String())
			}
			if test.status != 201 && f.calls != 0 {
				t.Fatal("unauthorized/invalid request reached use case")
			}
			if test.status == 201 && (f.command.IdempotencyKey != "original-key" || f.command.ProviderID != "provider-a") {
				t.Fatalf("command=%+v", f.command)
			}
		})
	}
}
func TestRoutesScopesAndResultStatuses(t *testing.T) {
	for _, prefix := range []string{"", "/v1"} {
		t.Run("wallet"+prefix, func(t *testing.T) {
			f := &fakeService{}
			h := testHandler(t, f, 0)
			r := httptest.NewRequest("POST", prefix+"/wallets", strings.NewReader(fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, testPlayer)))
			r.Header.Set("Authorization", "Bearer internal")
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 201 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	for _, test := range []struct {
		status   string
		replay   bool
		expected int
	}{{"PROCESSED", false, 201}, {"PROCESSED", true, 200}, {"PENDING_REFERENCE", false, 202}, {"REJECTED", false, 422}, {"REJECTED", true, 200}, {"FAILED", false, 503}} {
		t.Run(fmt.Sprint(test), func(t *testing.T) {
			f := &fakeService{result: application.Result{Status: test.status, IdempotentReplay: test.replay}}
			h := testHandler(t, f, 0)
			r := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(validBody))
			r.Header.Set("Authorization", "Bearer provider-a")
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "k")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != test.expected {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	for _, path := range []string{"/wallets/" + testWallet, "/providers/provider-b/wagering/transactions/external"} {
		f := &fakeService{}
		h := testHandler(t, f, 0)
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer provider-a")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 403 || f.calls != 0 {
			t.Fatalf("path=%s status=%d calls=%d", path, w.Code, f.calls)
		}
	}
}
func TestInternalErrorsNeverLeak(t *testing.T) {
	f := &fakeService{err: fmt.Errorf("postgres DSN password=secret: %w", errors.New("broken"))}
	h := testHandler(t, f, 0)
	r := httptest.NewRequest("GET", "/wallets/"+testWallet, nil)
	r.Header.Set("Authorization", "Bearer internal")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func TestDeterministicInputErrorsAreNotTransient(t *testing.T) {
	for _, code := range []string{"REFERENCE_REQUIRED", "PLAYER_MISMATCH"} {
		t.Run(code, func(t *testing.T) {
			f := &fakeService{err: &application.Error{Code: code}}
			h := testHandler(t, f, 0)
			r := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(validBody))
			r.Header.Set("Authorization", "Bearer provider-a")
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", "key")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 400 || !strings.Contains(w.Body.String(), code) {
				t.Fatalf("deterministic error classified incorrectly: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
