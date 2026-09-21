//go:build integration

package model_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"jungle/internal/application"
)

// op is one generated operation. The identity is what the engine deduplicates
// on, so the generator reuses identities on purpose to exercise replay.
type op struct {
	seq       int
	walletIdx int
	kind      string
	amount    string
	external  string
	key       string
	reference string // external id of the referenced operation, may not exist yet
}

// observation records what a single attempt saw, so conflicting answers to the
// same identity become visible.
type observation struct {
	status        string
	transactionID string
	balance       string
	version       int64
	failureCode   string
	code          string
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func number(t *testing.T, key string, fallback int) int {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s: %v", key, err)
	}
	return v
}

// generate builds an operation stream that leans into the combinations a
// hand-written test would not: reversals of reversals, references that do not
// exist yet, and identities deliberately reused with a changed payload.
func generate(rng *rand.Rand, wallets, count int) []op {
	ops := make([]op, 0, count)
	financial := make([]string, 0, count) // external ids worth referencing
	for i := 0; i < count; i++ {
		o := op{seq: i, walletIdx: rng.IntN(wallets)}
		o.external = fmt.Sprintf("model-%d", i)
		o.key = "key:" + o.external
		switch n := rng.IntN(100); {
		case n < 40:
			o.kind, o.amount = "BET", strconv.Itoa(1+rng.IntN(40))+".00"
		case n < 60:
			o.kind, o.amount = "WIN", strconv.Itoa(1+rng.IntN(30))+".00"
		case n < 68:
			o.kind, o.amount = "LOSS", "0.00"
		case n < 84 && len(financial) > 0:
			// A reversal of an arbitrary earlier operation, including of another
			// reversal: the engine must refuse the illegal ones, not crash.
			ref := financial[rng.IntN(len(financial))]
			o.kind, o.reference = map[bool]string{true: "REFUND", false: "ROLLBACK"}[rng.IntN(2) == 0], ref
			o.amount = strconv.Itoa(1+rng.IntN(40)) + ".00"
		case n < 92 && i > 0:
			// Same identity as an earlier operation. Same payload must replay;
			// a changed payload must be refused as a conflict, never applied.
			prev := ops[rng.IntN(len(ops))]
			o = prev
			o.seq = i
			if rng.IntN(2) == 0 {
				o.amount = strconv.Itoa(1+rng.IntN(40)) + ".00" // conflicting payload
			}
		default:
			// A reversal pointing at an identity that does not exist yet, which
			// must land in PENDING_REFERENCE rather than invent money.
			o.kind = "ROLLBACK"
			o.reference = fmt.Sprintf("model-%d", count+rng.IntN(count))
			o.amount = strconv.Itoa(1+rng.IntN(40)) + ".00"
		}
		if o.kind == "BET" || o.kind == "WIN" {
			financial = append(financial, o.external)
		}
		ops = append(ops, o)
	}
	return ops
}

// TestConcurrentConformance is the rigorous scenario: a generated, adversarial
// stream applied by many workers at once, then checked against the properties
// that must hold under every possible interleaving.
func TestConcurrentConformance(t *testing.T) {
	workers := number(t, "MODEL_WORKERS", runtime.NumCPU()*3)
	operations := number(t, "MODEL_OPS", 1500)
	wallets := number(t, "MODEL_WALLETS", 16)

	seed := uint64(time.Now().UnixNano())
	if raw := os.Getenv("MODEL_SEED"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			t.Fatalf("MODEL_SEED: %v", err)
		}
		seed = parsed
	}
	// Printed always, not only on failure: a conformance failure is worthless if
	// it cannot be replayed.
	t.Logf("seed=%d workers=%d operations=%d wallets=%d (replay with MODEL_SEED=%d)",
		seed, workers, operations, wallets, seed)

	f := database(t, int32(workers)+4)
	opened := make([]application.WalletView, 0, wallets)
	for i := 0; i < wallets; i++ {
		opened = append(opened, openWallet(t, f, "100000.00"))
	}

	rng := rand.New(rand.NewPCG(seed, seed>>32))
	ops := generate(rng, wallets, operations)

	var mu sync.Mutex
	seen := map[string][]observation{} // external id -> every answer observed

	queue := make(chan op)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o := range queue {
				wallet := opened[o.walletIdx]
				cmd := application.Command{
					ProviderID: "provider-a", ExternalTransactionID: o.external, IdempotencyKey: o.key,
					PlayerID: wallet.PlayerID, WalletID: wallet.ID, RoundID: "round", GameID: "game",
					Kind: o.kind, Money: money(t, o.amount), ReferenceExternalTransactionID: o.reference,
					CorrelationID: o.external,
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				result, err := f.store.ProcessWager(ctx, cmd, nil)
				cancel()
				obs := observation{status: result.Status, transactionID: result.TransactionID,
					balance: result.Balance.String(), version: result.WalletVersion, failureCode: result.FailureCode}
				if err != nil {
					obs.code = application.Code(err)
					var domainErr *application.Error
					if !errors.As(err, &domainErr) && obs.code == "UNAVAILABLE" {
						// An unclassified failure is the one outcome that means
						// the engine did not decide; surface it loudly.
						t.Errorf("operation %s failed without a classification: %v", o.external, err)
					}
				}
				mu.Lock()
				seen[o.external] = append(seen[o.external], obs)
				mu.Unlock()
			}
		}()
	}
	start := time.Now()
	for _, o := range ops {
		queue <- o
	}
	close(queue)
	wg.Wait()
	elapsed := time.Since(start)
	t.Logf("applied %d operations across %d wallets with %d workers in %s (%.0f ops/s)",
		len(ops), wallets, workers, elapsed.Round(time.Millisecond), float64(len(ops))/elapsed.Seconds())

	assertIdentityAgreement(t, seen)
	assertNoDuplicateEffect(t, f, seen)
	assertAuditorClean(t, f, seed)
}
