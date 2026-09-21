//go:build integration

package auth_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// These are real HTTP requests to the provisioned Keycloak and running API.
// Missing infrastructure fails this suite instead of silently skipping it.
func TestRealKeycloakProviderIsolation(t *testing.T) {
	api := env("TEST_API_URL", "http://localhost:8080")
	issuer := env("TEST_OIDC_ISSUER", "http://localhost:8081/realms/jungle")
	client := &http.Client{Timeout: 10 * time.Second}
	internal := token(t, client, issuer, "wallet-internal-client", "wallet-internal-local-only")
	providerA := token(t, client, issuer, "provider-a-client", "provider-a-local-only")
	providerB := token(t, client, issuer, "provider-b-client", "provider-b-local-only")
	player := uuid(t)
	walletBody := fmt.Sprintf(`{"playerId":%q,"initialBalance":{"amount":"100.00","currency":"BRL"}}`, player)
	request(t, client, "POST", api+"/wallets", "", walletBody, "", 401)
	request(t, client, "POST", api+"/wallets", "invalid", walletBody, "", 401)
	request(t, client, "POST", api+"/wallets", providerA, walletBody, "", 403)
	created := request(t, client, "POST", api+"/wallets", internal, walletBody, "", 201)
	var wallet struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created, &wallet); err != nil || wallet.ID == "" {
		t.Fatalf("wallet response: %s error=%v", created, err)
	}
	request(t, client, "GET", api+"/wallets/"+wallet.ID, providerA, "", "", 403)
	operation := fmt.Sprintf(`{"providerId":"provider-a","externalTransactionId":%q,"playerId":%q,"walletId":%q,"roundId":"auth-round","gameId":"auth-game","kind":"BET","money":{"amount":"1.00","currency":"BRL"}}`, "auth-"+uuid(t), player, wallet.ID)
	key := "auth-key-" + uuid(t)
	request(t, client, "POST", api+"/wagering/transactions", providerB, operation, key, 403)
	request(t, client, "POST", api+"/wagering/transactions", internal, operation, key, 403)
	processed := request(t, client, "POST", api+"/wagering/transactions", providerA, operation, key, 201)
	var result struct {
		ID     string `json:"transactionId"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(processed, &result); err != nil || result.ID == "" || result.Status != "PROCESSED" {
		t.Fatalf("transaction response: %s error=%v", processed, err)
	}
	request(t, client, "GET", api+"/wagering/transactions/"+result.ID, providerB, "", "", 404)
	request(t, client, "GET", api+"/providers/provider-a/wagering/transactions/external", providerB, "", "", 403)
	request(t, client, "POST", api+"/wagering/transactions", providerB, operation, key, 403)
	request(t, client, "POST", api+"/wagering/transactions", providerA, operation, key, 200)
	final := request(t, client, "GET", api+"/wallets/"+wallet.ID, internal, "", "", 200)
	var current struct {
		Balance struct {
			Amount string `json:"amount"`
		} `json:"balance"`
		Version int64 `json:"version"`
	}
	if err := json.Unmarshal(final, &current); err != nil || current.Balance.Amount != "99.00" || current.Version != 2 {
		t.Fatalf("unauthorized request or replay changed balance: %s error=%v", final, err)
	}
}

func token(t *testing.T, client *http.Client, issuer, id, secret string) string {
	t.Helper()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {secret}}
	response, err := client.PostForm(issuer+"/protocol/openid-connect/token", form)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("Keycloak token status=%d for %s", response.StatusCode, id)
	}
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&body); err != nil || body.AccessToken == "" {
		t.Fatalf("Keycloak token missing: %v", err)
	}
	return body.AccessToken
}
func request(t *testing.T, client *http.Client, method, address, token, body, key string, status int) []byte {
	t.Helper()
	r, err := http.NewRequest(method, address, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status {
		t.Fatalf("%s %s status=%d expected=%d body=%s", method, address, response.StatusCode, status, data)
	}
	return data
}
func uuid(t *testing.T) string {
	t.Helper()
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	id[6] = (id[6] & 15) | 64
	id[8] = (id[8] & 63) | 128
	s := hex.EncodeToString(id[:])
	return strings.Join([]string{s[:8], s[8:12], s[12:16], s[16:20], s[20:]}, "-")
}
func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return strings.TrimRight(value, "/")
	}
	return fallback
}
