package domain

import (
	"math"
	"time"
)

type WalletData struct {
	ID        WalletID  `json:"id"`
	PlayerID  PlayerID  `json:"playerId"`
	Balance   Money     `json:"balance"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Wallet struct{ data WalletData }

func validateWallet(data WalletData) error {
	if data.ID == (WalletID{}) || data.PlayerID == (PlayerID{}) || !data.Balance.Valid() ||
		data.Balance.Minor() < 0 || data.Version < 1 || data.CreatedAt.IsZero() || data.UpdatedAt.Before(data.CreatedAt) {
		return invalid(CodeInvalidWallet, "wallet identity, nonnegative balance, version and timestamps are required")
	}
	return nil
}

func NewWallet(data WalletData) (Wallet, error) {
	if data.Version != 1 {
		return Wallet{}, invalid(CodeInvalidWallet, "initial wallet version must be one")
	}
	return RehydrateWallet(data)
}

func RehydrateWallet(data WalletData) (Wallet, error) {
	if err := validateWallet(data); err != nil {
		return Wallet{}, err
	}
	return Wallet{data: data}, nil
}

func (w Wallet) Snapshot() WalletData { return w.data }

func (w *Wallet) Credit(money Money, now time.Time) error { return w.move(money, now, false) }
func (w *Wallet) Debit(money Money, now time.Time) error  { return w.move(money, now, true) }

func (w *Wallet) move(money Money, now time.Time, debit bool) error {
	if w == nil {
		return invalid(CodeInvalidWallet, "wallet is nil")
	}
	if err := validateWallet(w.data); err != nil {
		return err
	}
	if err := w.data.Balance.check(money); err != nil {
		return err
	}
	if money.Minor() < 0 {
		return invalid(CodeInvalidAmount, "movement must be nonnegative")
	}
	if now.IsZero() || now.Before(w.data.UpdatedAt) {
		return invalid(CodeInvalidWallet, "movement timestamp precedes wallet state")
	}
	if money.Minor() == 0 {
		return nil
	}
	if w.data.Version == math.MaxInt64 {
		return invalid(CodeOverflow, "wallet version overflows")
	}
	var next Money
	var err error
	if debit {
		if money.Minor() > w.data.Balance.Minor() {
			return invalid(CodeInsufficientFunds, "insufficient wallet balance")
		}
		next, err = w.data.Balance.Subtract(money)
	} else {
		next, err = w.data.Balance.Add(money)
	}
	if err != nil {
		return err
	}
	w.data.Balance, w.data.Version, w.data.UpdatedAt = next, w.data.Version+1, now
	return nil
}
