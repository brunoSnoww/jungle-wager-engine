package domain

import (
	"encoding/json"
	"strings"
	"time"
)

const (
	EventWagerProcessed        = "WagerTransactionProcessed"
	EventWagerRejected         = "WagerTransactionRejected"
	EventWagerPendingReference = "WagerTransactionPendingReference"
	EventWagerFailed           = "WagerTransactionFailed"
	EventWalletBalanceChanged  = "WalletBalanceChanged"
)

type EventMeta struct {
	ID            EventID
	CorrelationID string
	CausationID   string
	OccurredAt    time.Time
}

// Event keeps the validated, serialized envelope private. Returning a copy of
// its JSON prevents an outbox caller from changing a payload after creation.
type Event struct {
	id          EventID
	aggregateID WalletID
	eventType   string
	occurredAt  time.Time
	payload     []byte
}

func (e Event) ID() EventID           { return e.id }
func (e Event) AggregateID() WalletID { return e.aggregateID }
func (e Event) Type() string          { return e.eventType }
func (e Event) OccurredAt() time.Time { return e.occurredAt }
func (e Event) Version() int          { return 1 }
func (e Event) MarshalJSON() ([]byte, error) {
	if e.id == (EventID{}) || len(e.payload) == 0 {
		return nil, invalid(CodeInvalidEvent, "uninitialized event")
	}
	return append([]byte(nil), e.payload...), nil
}

type wagerEventData struct {
	TransactionID                  WagerTransactionID `json:"transactionId"`
	WalletID                       WalletID           `json:"walletId"`
	PlayerID                       PlayerID           `json:"playerId"`
	ProviderID                     string             `json:"providerId,omitempty"`
	ExternalTransactionID          string             `json:"externalTransactionId,omitempty"`
	RoundID                        string             `json:"roundId,omitempty"`
	GameID                         string             `json:"gameId,omitempty"`
	Kind                           Kind               `json:"kind"`
	Money                          Money              `json:"money"`
	Status                         Status             `json:"status"`
	FailureCode                    string             `json:"failureCode,omitempty"`
	Balance                        *Money             `json:"balance,omitempty"`
	WalletVersion                  int64              `json:"walletVersion,omitempty"`
	ReferenceExternalTransactionID string             `json:"referenceExternalTransactionId,omitempty"`
}

// Distinct payload types make each versioned integration contract explicit.
type WagerTransactionProcessedData wagerEventData
type WagerTransactionRejectedData wagerEventData
type WagerTransactionPendingReferenceData wagerEventData
type WagerTransactionFailedData wagerEventData
type WalletBalanceChangedData struct {
	WalletID      WalletID           `json:"walletId"`
	TransactionID WagerTransactionID `json:"transactionId"`
	Direction     Direction          `json:"direction"`
	Money         Money              `json:"money"`
	BalanceBefore Money              `json:"balanceBefore"`
	BalanceAfter  Money              `json:"balanceAfter"`
	WalletVersion int64              `json:"walletVersion"`
}
type wagerPayload interface {
	WagerTransactionProcessedData | WagerTransactionRejectedData | WagerTransactionPendingReferenceData | WagerTransactionFailedData
}
type eventPayload interface {
	wagerPayload | WalletBalanceChangedData
}
type Envelope[T eventPayload] struct {
	EventID       EventID   `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   WalletID  `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          T         `json:"data"`
}

func newEvent[T eventPayload](meta EventMeta, aggregateID WalletID, eventType string, data T) (Event, error) {
	if meta.ID == (EventID{}) || aggregateID == (WalletID{}) || meta.OccurredAt.IsZero() || strings.TrimSpace(meta.CorrelationID) == "" {
		return Event{}, invalid(CodeInvalidEvent, "event identity, aggregate, occurrence and correlation required")
	}
	envelope := Envelope[T]{meta.ID, eventType, aggregateID, meta.CorrelationID, meta.CausationID, meta.OccurredAt.UTC(), 1, data}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return Event{}, err
	}
	return Event{meta.ID, aggregateID, eventType, meta.OccurredAt.UTC(), payload}, nil
}

func newWagerEvent[T wagerPayload](meta EventMeta, wager WagerData, expected Status, eventType string) (Event, error) {
	if err := validateWager(wager); err != nil {
		return Event{}, err
	}
	if wager.Status != expected {
		return Event{}, invalid(CodeInvalidEvent, "event does not match transaction state")
	}
	data := wagerEventData{
		TransactionID: wager.ID, WalletID: wager.WalletID, PlayerID: wager.PlayerID,
		ProviderID: wager.ProviderID, ExternalTransactionID: wager.ExternalTransactionID,
		RoundID: wager.RoundID, GameID: wager.GameID, Kind: wager.Kind, Money: wager.Money,
		Status: wager.Status, FailureCode: wager.FailureCode, WalletVersion: wager.ResultVersion,
		ReferenceExternalTransactionID: wager.ReferenceExternalTransactionID,
	}
	if wager.ResultBalance.Valid() {
		balance := wager.ResultBalance
		data.Balance = &balance
	}
	return newEvent(meta, wager.WalletID, eventType, T(data))
}

func NewWagerProcessed(meta EventMeta, wager WagerData) (Event, error) {
	return newWagerEvent[WagerTransactionProcessedData](meta, wager, StatusProcessed, EventWagerProcessed)
}
func NewWagerRejected(meta EventMeta, wager WagerData) (Event, error) {
	return newWagerEvent[WagerTransactionRejectedData](meta, wager, StatusRejected, EventWagerRejected)
}
func NewWagerPendingReference(meta EventMeta, wager WagerData) (Event, error) {
	return newWagerEvent[WagerTransactionPendingReferenceData](meta, wager, StatusPendingReference, EventWagerPendingReference)
}
func NewWagerFailed(meta EventMeta, wager WagerData) (Event, error) {
	return newWagerEvent[WagerTransactionFailedData](meta, wager, StatusFailed, EventWagerFailed)
}

func NewWalletBalanceChanged(meta EventMeta, entry LedgerData, walletVersion int64) (Event, error) {
	if _, err := NewLedgerEntry(entry); err != nil {
		return Event{}, err
	}
	if walletVersion < 1 {
		return Event{}, invalid(CodeInvalidEvent, "wallet version must be positive")
	}
	data := WalletBalanceChangedData{entry.WalletID, entry.TransactionID, entry.Direction, entry.Money, entry.BalanceBefore, entry.BalanceAfter, walletVersion}
	return newEvent(meta, entry.WalletID, EventWalletBalanceChanged, data)
}
