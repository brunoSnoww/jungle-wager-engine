// Package auth verifies external client-credentials tokens. It never trusts a
// token's URL headers or its provider claim before signature validation.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var ErrInvalidToken = errors.New("invalid access token")

type Principal struct {
	Subject    string
	ProviderID string
	Scopes     map[string]bool
}

func (p Principal) HasScope(scope string) bool { return p.Scopes[scope] }

type Config struct {
	Issuer string
	// FetchIssuer is an explicitly configured internal route to the same IdP.
	// The signed issuer must still equal Issuer exactly.
	FetchIssuer       string
	Audience          string
	AllowInsecureHTTP bool // Local development only; never derived from a request.
	CacheTTL          time.Duration
}

type Verifier struct {
	config      Config
	client      *http.Client
	mu          sync.Mutex
	keys        map[string]*rsa.PublicKey
	loadedAt    time.Time
	lastAttempt time.Time
}

func New(config Config, client *http.Client) (*Verifier, error) {
	if config.Audience == "" {
		return nil, errors.New("OIDC audience is required")
	}
	config.Issuer = strings.TrimRight(config.Issuer, "/")
	if config.FetchIssuer == "" {
		config.FetchIssuer = config.Issuer
	}
	config.FetchIssuer = strings.TrimRight(config.FetchIssuer, "/")
	for _, address := range []string{config.Issuer, config.FetchIssuer} {
		u, err := url.Parse(address)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(config.AllowInsecureHTTP && u.Scheme == "http")) {
			return nil, errors.New("OIDC issuer must be a trusted HTTPS URL (HTTP requires explicit local setting)")
		}
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute
	}
	if config.CacheTTL < time.Second {
		return nil, errors.New("OIDC cache TTL must be at least one second")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return errors.New("OIDC redirects are not allowed") }
	if copyClient.Timeout == 0 {
		copyClient.Timeout = 5 * time.Second
	}
	return &Verifier{config: config, client: &copyClient, keys: make(map[string]*rsa.PublicKey)}, nil
}

// Check loads keys before the service becomes ready. Network requests are
// bounded and restricted to the configured identity provider.
func (v *Verifier) Check(ctx context.Context) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.refresh(ctx)
}

type accessClaims struct {
	jwt.RegisteredClaims
	ProviderID string `json:"provider_id"`
	Scope      string `json:"scope"`
}

func (v *Verifier) Validate(ctx context.Context, raw string) (Principal, error) {
	if len(raw) == 0 || len(raw) > 8192 {
		return Principal{}, ErrInvalidToken
	}
	claims := &accessClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) {
		kid, ok := token.Header["kid"].(string)
		if !ok || kid == "" || len(kid) > 256 || token.Method != jwt.SigningMethodRS256 {
			return nil, ErrInvalidToken
		}
		return v.key(ctx, kid)
	}, jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(v.config.Issuer), jwt.WithAudience(v.config.Audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithLeeway(5*time.Second))
	if err != nil || !token.Valid || claims.Subject == "" {
		return Principal{}, ErrInvalidToken
	}
	principal := Principal{Subject: claims.Subject, ProviderID: claims.ProviderID, Scopes: make(map[string]bool)}
	for _, scope := range strings.Fields(claims.Scope) {
		principal.Scopes[scope] = true
	}
	return principal, nil
}

func (v *Verifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	key := v.keys[kid]
	if key != nil && time.Since(v.loadedAt) < v.config.CacheTTL {
		return key, nil
	}
	// Bound attacker-triggered unknown-kid refreshes. An unknown key is never
	// accepted while waiting for refresh; the client can safely retry.
	if time.Since(v.lastAttempt) < time.Second {
		return nil, ErrInvalidToken
	}
	if err := v.refresh(ctx); err != nil {
		return nil, err
	}
	if key = v.keys[kid]; key == nil {
		return nil, ErrInvalidToken
	}
	return key, nil
}

func (v *Verifier) refresh(ctx context.Context) error {
	v.lastAttempt = time.Now()
	var discovery struct {
		Issuer  string `json:"issuer"`
		JWKSURI string `json:"jwks_uri"`
	}
	if err := v.get(ctx, v.config.FetchIssuer+"/.well-known/openid-configuration", &discovery); err != nil {
		return err
	}
	if discovery.Issuer != v.config.Issuer {
		return errors.New("OIDC discovery issuer mismatch")
	}
	// Only a path beneath this exact issuer may be fetched, even if discovery
	// is compromised. Explicit FetchIssuer translation handles Compose routing.
	if !strings.HasPrefix(discovery.JWKSURI, v.config.Issuer+"/") {
		return errors.New("OIDC JWKS location outside configured issuer")
	}
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	address := v.config.FetchIssuer + strings.TrimPrefix(discovery.JWKSURI, v.config.Issuer)
	if err := v.get(ctx, address, &set); err != nil {
		return err
	}
	keys := make(map[string]*rsa.PublicKey)
	for _, k := range set.Keys {
		if k.Kty != "RSA" || k.Kid == "" || (k.Use != "" && k.Use != "sig") || (k.Alg != "" && k.Alg != "RS256") {
			continue
		}
		n, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			continue
		}
		e, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil || len(e) == 0 || len(e) > 4 {
			continue
		}
		exponent := new(big.Int).SetBytes(e).Int64()
		modulus := new(big.Int).SetBytes(n)
		if exponent < 3 || exponent > 2147483647 || exponent%2 == 0 || modulus.BitLen() < 2048 {
			continue
		}
		if _, duplicate := keys[k.Kid]; duplicate {
			return errors.New("duplicate OIDC signing key ID")
		}
		keys[k.Kid] = &rsa.PublicKey{N: modulus, E: int(exponent)}
	}
	if len(keys) == 0 {
		return errors.New("OIDC has no usable RS256 signing key")
	}
	v.keys, v.loadedAt = keys, time.Now()
	return nil
}

func (v *Verifier) get(ctx context.Context, address string, destination any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return fmt.Errorf("OIDC request: %w", err)
	}
	response, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("OIDC fetch: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("OIDC returned status %d", response.StatusCode)
	}
	const limit = 1 << 20
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return fmt.Errorf("OIDC response: %w", err)
	}
	if len(body) > limit {
		return errors.New("OIDC response exceeds limit")
	}
	if err := json.Unmarshal(body, destination); err != nil {
		return fmt.Errorf("OIDC JSON: %w", err)
	}
	return nil
}
