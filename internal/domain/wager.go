package domain

import (
	"encoding/hex"
	"strings"
	"time"
)

type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBET      Kind = "BET"
	KindWIN      Kind = "WIN"
	KindLOSS     Kind = "LOSS"
	KindREFUND   Kind = "REFUND"
	KindROLLBACK Kind = "ROLLBACK"
)

type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func (s Status) Terminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

type Direction string

const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
	DirectionNone   Direction = "NONE"
)

type WagerData struct {
	ID                             WagerTransactionID
	WalletID                       WalletID
	PlayerID                       PlayerID
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          Money
	ReferenceExternalTransactionID string
	ReferenceID                    WagerTransactionID
	Status                         Status
	FailureCode                    string
	ResultBalance                  Money
	ResultVersion                  int64
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	CompletedAt                    *time.Time
}

type WagerTransaction struct{ data WagerData }

func externalKind(kind Kind) bool {
	return kind == KindBET || kind == KindWIN || kind == KindLOSS || kind == KindREFUND || kind == KindROLLBACK
}

func validateWager(data WagerData) error {
	if data.ID == (WagerTransactionID{}) || data.WalletID == (WalletID{}) || data.PlayerID == (PlayerID{}) ||
		!data.Money.Valid() || data.Money.Minor() < 0 || data.CreatedAt.IsZero() || data.UpdatedAt.Before(data.CreatedAt) {
		return invalid(CodeInvalidWager, "identity, nonnegative money and timestamps required")
	}
	if data.Kind == KindOpening {
		if data.Money.Minor() <= 0 || data.ProviderID != "" || data.ExternalTransactionID != "" ||
			data.IdempotencyKey != "" || data.PayloadHash != "" || data.RoundID != "" || data.GameID != "" ||
			data.ReferenceExternalTransactionID != "" || data.ReferenceID != (WagerTransactionID{}) {
			return invalid(CodeInvalidWager, "opening has positive money and no external metadata")
		}
	} else {
		if !externalKind(data.Kind) || strings.TrimSpace(data.ProviderID) == "" || strings.TrimSpace(data.ExternalTransactionID) == "" ||
			strings.TrimSpace(data.IdempotencyKey) == "" || strings.TrimSpace(data.RoundID) == "" || strings.TrimSpace(data.GameID) == "" {
			return invalid(CodeInvalidWager, "external transaction metadata required")
		}
		decoded, err := hex.DecodeString(data.PayloadHash)
		if err != nil || len(decoded) != 32 {
			return invalid(CodeInvalidWager, "payload hash must be SHA-256 hexadecimal")
		}
		if (data.Kind == KindREFUND || data.Kind == KindROLLBACK) && strings.TrimSpace(data.ReferenceExternalTransactionID) == "" {
			return invalid(CodeInvalidWager, "reversal reference required")
		}
		if (data.Kind == KindBET || data.Kind == KindLOSS) && data.ReferenceExternalTransactionID != "" {
			return invalid(CodeInvalidWager, "kind does not accept a reference")
		}
	}
	if data.Status != StatusPending && data.Status != StatusPendingReference && !data.Status.Terminal() {
		return invalid(CodeInvalidState, "unknown transaction state")
	}
	if data.Status == StatusPendingReference && data.ReferenceExternalTransactionID == "" {
		return invalid(CodeInvalidState, "pending reference requires external reference")
	}
	if data.Status.Terminal() {
		if data.CompletedAt == nil || data.CompletedAt.Before(data.CreatedAt) || data.CompletedAt.After(data.UpdatedAt) {
			return invalid(CodeInvalidState, "terminal state requires completion timestamp")
		}
		if data.Status == StatusProcessed && (data.FailureCode != "" || !data.ResultBalance.Valid() || data.ResultBalance.Minor() < 0 ||
			data.ResultBalance.Currency() != data.Money.Currency() || data.ResultVersion < 1) {
			return invalid(CodeInvalidState, "processed state requires a valid financial result")
		}
		if data.Status == StatusProcessed {
			if data.Kind == KindLOSS && data.Money.Minor() != 0 || data.Kind != KindLOSS && data.Money.Minor() <= 0 {
				return invalid(CodeInvalidAmountForKind, "processed amount incompatible with transaction kind")
			}
			if data.ReferenceExternalTransactionID != "" && data.ReferenceID == (WagerTransactionID{}) {
				return invalid(CodeInvalidState, "processed reference must be resolved")
			}
		}
		if (data.Status == StatusRejected || data.Status == StatusFailed) && strings.TrimSpace(data.FailureCode) == "" {
			return invalid(CodeInvalidState, "failure code required for terminal failure")
		}
		if (data.Status == StatusRejected || data.Status == StatusFailed) && (!data.ResultBalance.Valid() || data.ResultBalance.Minor() < 0 || data.ResultVersion < 1) {
			return invalid(CodeInvalidState, "terminal failure requires original wallet result")
		}
	} else if data.CompletedAt != nil || data.FailureCode != "" || data.ResultVersion != 0 || data.ResultBalance.Valid() {
		return invalid(CodeInvalidState, "nonterminal state must not contain terminal result")
	}
	return nil
}

// NewWager constructs only external commands; OPENING has a separate entry point.
func NewWager(data WagerData) (WagerTransaction, error) {
	if data.Kind == KindOpening {
		return WagerTransaction{}, invalid(CodeInvalidWager, "OPENING is internal only")
	}
	if data.Status != StatusPending {
		return WagerTransaction{}, invalid(CodeInvalidState, "new transaction must be PENDING")
	}
	return RehydrateWager(data)
}

func NewOpening(data WagerData) (WagerTransaction, error) {
	if data.Kind != KindOpening || data.Status != StatusPending {
		return WagerTransaction{}, invalid(CodeInvalidWager, "new opening must be PENDING OPENING")
	}
	return RehydrateWager(data)
}

// RehydrateWager validates an existing snapshot without transitions or events.
func RehydrateWager(data WagerData) (WagerTransaction, error) {
	if err := validateWager(data); err != nil {
		return WagerTransaction{}, err
	}
	return WagerTransaction{data: cloneWager(data)}, nil
}

func cloneWager(data WagerData) WagerData {
	if data.CompletedAt != nil {
		at := *data.CompletedAt
		data.CompletedAt = &at
	}
	return data
}

func (w WagerTransaction) Snapshot() WagerData { return cloneWager(w.data) }

func (w *WagerTransaction) transition(now time.Time, change func(*WagerData)) error {
	if w == nil {
		return invalid(CodeInvalidWager, "transaction is nil")
	}
	if err := validateWager(w.data); err != nil {
		return err
	}
	if w.data.Status.Terminal() {
		return invalid(CodeInvalidState, "terminal transaction cannot transition")
	}
	if now.IsZero() || now.Before(w.data.UpdatedAt) {
		return invalid(CodeInvalidState, "transition timestamp precedes transaction state")
	}
	next := cloneWager(w.data)
	next.UpdatedAt = now
	change(&next)
	if next.Status.Terminal() {
		at := now
		next.CompletedAt = &at
	}
	if err := validateWager(next); err != nil {
		return err
	}
	w.data = next
	return nil
}

func (w *WagerTransaction) PendingReference(now time.Time) error {
	return w.transition(now, func(data *WagerData) { data.Status = StatusPendingReference })
}

func (w *WagerTransaction) Process(balance Money, version int64, referenceID WagerTransactionID, now time.Time) error {
	return w.transition(now, func(data *WagerData) {
		data.Status, data.ResultBalance, data.ResultVersion, data.ReferenceID = StatusProcessed, balance, version, referenceID
	})
}

func (w *WagerTransaction) Reject(code string, balance Money, version int64, now time.Time) error {
	return w.transition(now, func(data *WagerData) {
		data.Status, data.FailureCode, data.ResultBalance, data.ResultVersion = StatusRejected, code, balance, version
	})
}

func (w *WagerTransaction) Fail(code string, balance Money, version int64, now time.Time) error {
	return w.transition(now, func(data *WagerData) {
		data.Status, data.FailureCode, data.ResultBalance, data.ResultVersion = StatusFailed, code, balance, version
	})
}

type Decision struct {
	Direction   Direction
	Pending     bool
	FailureCode string
}

// Evaluate decides the financial effect without mutating either aggregate. A
// business rejection is data so the caller can commit its audit and event.
func Evaluate(wallet WalletData, wager WagerData, reference *WagerData, alreadyReversed bool) (Decision, error) {
	if err := validateWallet(wallet); err != nil {
		return Decision{}, err
	}
	if err := validateWager(wager); err != nil {
		return Decision{}, err
	}
	if wager.Status.Terminal() {
		return Decision{}, invalid(CodeInvalidState, "cannot evaluate terminal transaction")
	}
	if wager.WalletID != wallet.ID || wager.PlayerID != wallet.PlayerID {
		return Decision{}, invalid(CodeInvalidWager, "transaction does not belong to wallet and player")
	}
	reject := func(code string) (Decision, error) { return Decision{Direction: DirectionNone, FailureCode: code}, nil }
	if wager.Money.Currency() != wallet.Balance.Currency() {
		return reject(CodeCurrencyMismatch)
	}
	if wager.Kind == KindLOSS && wager.Money.Minor() != 0 || wager.Kind != KindLOSS && wager.Money.Minor() <= 0 {
		return reject(CodeInvalidAmountForKind)
	}
	if wager.Kind == KindOpening {
		return Decision{}, invalid(CodeInvalidWager, "opening must use wallet creation")
	}
	reversal := wager.Kind == KindREFUND || wager.Kind == KindROLLBACK
	if wager.ReferenceExternalTransactionID != "" {
		if reference == nil {
			return Decision{Direction: DirectionNone, Pending: true}, nil
		}
		if reference.ID == wager.ID || reference.ProviderID != wager.ProviderID || reference.PlayerID != wager.PlayerID ||
			reference.WalletID != wager.WalletID || reference.Money.Currency() != wager.Money.Currency() || reference.RoundID != wager.RoundID ||
			reference.ExternalTransactionID != wager.ReferenceExternalTransactionID {
			return reject(CodeReferenceMismatch)
		}
		if wager.Kind == KindREFUND && reference.Kind != KindBET || wager.Kind == KindWIN && reference.Kind != KindBET ||
			wager.Kind == KindROLLBACK && reference.Kind != KindBET && reference.Kind != KindWIN && reference.Kind != KindREFUND {
			return reject(CodeReferenceMismatch)
		}
		if reversal && reference.Money.Minor() != wager.Money.Minor() {
			return reject(CodeReferenceMismatch)
		}
		if reference.Status == StatusPending || reference.Status == StatusPendingReference {
			return Decision{Direction: DirectionNone, Pending: true}, nil
		}
		if reference.Status != StatusProcessed {
			return reject(CodeReferenceNotProcessed)
		}
		if reversal && alreadyReversed {
			return reject(CodeReferenceAlreadyReversed)
		}
	}
	direction := DirectionNone
	switch wager.Kind {
	case KindBET:
		direction = DirectionDebit
	case KindWIN, KindREFUND:
		direction = DirectionCredit
	case KindROLLBACK:
		if reference.Kind == KindBET {
			direction = DirectionCredit
		} else {
			direction = DirectionDebit
		}
	}
	if direction == DirectionDebit && wager.Money.Minor() > wallet.Balance.Minor() {
		if reversal {
			return reject(CodeInsufficientFundsForReversal)
		}
		return reject(CodeInsufficientFunds)
	}
	if direction == DirectionCredit {
		if _, err := wallet.Balance.Add(wager.Money); err != nil {
			return reject(CodeOverflow)
		}
	}
	return Decision{Direction: direction}, nil
}
