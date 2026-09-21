// Package application defines the transport-independent boundary shared by HTTP
// and SQS. Financial invariants live in domain; persistence implements these ports.
package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"jungle/internal/domain"
)

type CreateWalletCommand struct {
	PlayerID       string       `json:"playerId"`
	InitialBalance domain.Money `json:"initialBalance"`
	CorrelationID  string       `json:"-"`
}

type Command struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	IdempotencyKey                 string       `json:"idempotencyKey,omitempty"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	CorrelationID                  string       `json:"-"`
	CausationID                    string       `json:"-"`
}

type WalletView struct {
	ID        string       `json:"id"`
	PlayerID  string       `json:"playerId"`
	Balance   domain.Money `json:"balance"`
	Version   int64        `json:"version"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

type Result struct {
	TransactionID    string       `json:"transactionId"`
	Status           string       `json:"status"`
	Balance          domain.Money `json:"balance"`
	WalletVersion    int64        `json:"walletVersion"`
	FailureCode      string       `json:"failureCode,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

type LedgerEntry struct {
	ID            string       `json:"id"`
	WalletID      string       `json:"walletId"`
	TransactionID string       `json:"transactionId"`
	Direction     string       `json:"direction"`
	Money         domain.Money `json:"money"`
	BalanceBefore domain.Money `json:"balanceBefore"`
	BalanceAfter  domain.Money `json:"balanceAfter"`
	CreatedAt     time.Time    `json:"createdAt"`
}

type LedgerPage struct {
	Entries    []LedgerEntry `json:"entries"`
	NextCursor string        `json:"nextCursor,omitempty"`
}

type Reconciliation struct {
	WalletID          string       `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int64        `json:"checkedEntries"`
}

// MaxOutboxBatch bounds one outbox claim. Config validation and the store guard
// must agree on it: a config that accepts more than the store allows passes
// startup and then fails every claim, silencing the publisher for good.
const MaxOutboxBatch = 100

type InboxMessage struct{ ConsumerName, MessageID, PayloadHash string }
type OutboxRecord struct {
	EventID, EventType, AggregateID, ClaimToken string
	Payload                                     []byte
	Attempts                                    int
}
type ReferenceClaim struct {
	TransactionID, WalletID, ClaimToken string
	Attempts                            int
	CreatedAt                           time.Time
}
type Backlog struct {
	OutboxCount, ReferenceCount int64
	OldestOutboxAge             time.Duration
}

type Wallets interface {
	CreateWallet(context.Context, CreateWalletCommand) (WalletView, error)
	GetWallet(context.Context, string) (WalletView, error)
	Ledger(context.Context, string, string, int) (LedgerPage, error)
	Reconcile(context.Context, string) (Reconciliation, error)
}
type Wagers interface {
	ProcessWager(context.Context, Command, *InboxMessage) (Result, error)
	GetTransaction(context.Context, string, string) (Result, error)
	GetExternalTransaction(context.Context, string, string) (Result, error)
}

type Error struct{ Code string }

func (e *Error) Error() string { return e.Code }
func Code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	var d *domain.Error
	if errors.As(err, &d) {
		return d.Code
	}
	return "UNAVAILABLE"
}

// PayloadHash uses encoding/json's lexicographic map-key ordering. It deliberately
// excludes the transport idempotency key: changing it is an external-ID conflict.
func PayloadHash(c Command) (string, error) {
	b, err := json.Marshal(map[string]any{
		"providerId": c.ProviderID, "externalTransactionId": c.ExternalTransactionID,
		"playerId": c.PlayerID, "walletId": c.WalletID, "roundId": c.RoundID,
		"gameId": c.GameID, "kind": c.Kind, "money": c.Money,
		"referenceExternalTransactionId": c.ReferenceExternalTransactionID,
	})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// InboxHash has different semantics: key and occurrence are part of a message's
// identity. The consumer normalizes DTO values before invoking this function.
func InboxHash(messageType string, occurredAt time.Time, c Command) (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return "", err
	}
	b, err := json.Marshal(map[string]any{"type": messageType, "occurredAt": occurredAt.UTC().Format(time.RFC3339Nano), "data": fields})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
