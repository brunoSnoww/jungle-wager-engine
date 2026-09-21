package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerifierValidatesSignatureAndClaims(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var issuer string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/realms/jungle/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer, "jwks_uri": issuer + "/certs"})
		case "/realms/jungle/certs":
			_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{"kid": "test-key", "kty": "RSA", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	issuer = server.URL + "/realms/jungle"
	verifier, err := New(Config{Issuer: issuer, Audience: "jungle-api", AllowInsecureHTTP: true}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(jwt.MapClaims)
		method jwt.SigningMethod
		key    any
		kid    string
		valid  bool
	}{
		{name: "valid", valid: true},
		{name: "expired", change: func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Minute).Unix() }},
		{name: "missing expiry", change: func(c jwt.MapClaims) { delete(c, "exp") }},
		{name: "wrong issuer", change: func(c jwt.MapClaims) { c["iss"] = "http://attacker.invalid" }},
		{name: "wrong audience", change: func(c jwt.MapClaims) { c["aud"] = "another-service" }},
		{name: "future issue", change: func(c jwt.MapClaims) { c["iat"] = time.Now().Add(time.Hour).Unix() }},
		{name: "future not before", change: func(c jwt.MapClaims) { c["nbf"] = time.Now().Add(time.Hour).Unix() }},
		{name: "missing subject", change: func(c jwt.MapClaims) { delete(c, "sub") }},
		{name: "unknown kid", kid: "untrusted"},
		{name: "wrong algorithm", method: jwt.SigningMethodHS256, key: []byte("secret")},
	} {
		t.Run(test.name, func(t *testing.T) {
			claims := jwt.MapClaims{"iss": issuer, "aud": "jungle-api", "sub": "service-account-a", "exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(), "provider_id": "provider-a", "scope": "wager:write wager:read"}
			if test.change != nil {
				test.change(claims)
			}
			method := test.method
			if method == nil {
				method = jwt.SigningMethodRS256
			}
			signingKey := test.key
			if signingKey == nil {
				signingKey = key
			}
			token := jwt.NewWithClaims(method, claims)
			token.Header["kid"] = "test-key"
			if test.kid != "" {
				token.Header["kid"] = test.kid
			}
			raw, err := token.SignedString(signingKey)
			if err != nil {
				t.Fatal(err)
			}
			principal, err := verifier.Validate(context.Background(), raw)
			if test.valid {
				if err != nil || principal.ProviderID != "provider-a" || !principal.HasScope("wager:write") {
					t.Fatalf("principal=%+v error=%v", principal, err)
				}
			} else if err == nil {
				t.Fatal("invalid token accepted")
			}
		})
	}
}

func TestVerifierRejectsUntrustedDiscovery(t *testing.T) {
	var issuer string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"issuer": issuer, "jwks_uri": "https://untrusted.invalid/certs"})
	}))
	defer server.Close()
	issuer = server.URL
	verifier, err := New(Config{Issuer: issuer, Audience: "jungle-api", AllowInsecureHTTP: true}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Check(context.Background()); err == nil {
		t.Fatal("untrusted JWKS URI accepted")
	}
	if _, err := New(Config{Issuer: issuer, Audience: "jungle-api"}, nil); err == nil {
		t.Fatal("HTTP accepted without explicit local opt-in")
	}
}
