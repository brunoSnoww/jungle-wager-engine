package domain

import "time"

type LedgerData struct {
	ID            LedgerEntryID      `json:"id"`
	WalletID      WalletID           `json:"walletId"`
	TransactionID WagerTransactionID `json:"transactionId"`
	Direction     Direction          `json:"direction"`
	Money         Money              `json:"money"`
	BalanceBefore Money              `json:"balanceBefore"`
	BalanceAfter  Money              `json:"balanceAfter"`
	CreatedAt     time.Time          `json:"createdAt"`
}

type WalletLedgerEntry struct{ data LedgerData }

func NewLedgerEntry(data LedgerData) (WalletLedgerEntry, error) {
	if data.ID == (LedgerEntryID{}) || data.WalletID == (WalletID{}) || data.TransactionID == (WagerTransactionID{}) || data.CreatedAt.IsZero() ||
		!data.Money.Valid() || data.Money.Minor() <= 0 || !data.BalanceBefore.Valid() || !data.BalanceAfter.Valid() ||
		data.BalanceBefore.Minor() < 0 || data.BalanceAfter.Minor() < 0 {
		return WalletLedgerEntry{}, invalid(CodeInvalidLedger, "ledger requires identities, positive money, nonnegative balances and timestamp")
	}
	var expected Money
	var err error
	switch data.Direction {
	case DirectionDebit:
		expected, err = data.BalanceBefore.Subtract(data.Money)
	case DirectionCredit:
		expected, err = data.BalanceBefore.Add(data.Money)
	default:
		return WalletLedgerEntry{}, invalid(CodeInvalidLedger, "ledger direction must be debit or credit")
	}
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	comparison, err := expected.Compare(data.BalanceAfter)
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	if comparison != 0 {
		return WalletLedgerEntry{}, invalid(CodeInvalidLedger, "ledger before/amount/after equation does not balance")
	}
	return WalletLedgerEntry{data: data}, nil
}

func RehydrateLedgerEntry(data LedgerData) (WalletLedgerEntry, error) { return NewLedgerEntry(data) }
func (entry WalletLedgerEntry) Snapshot() LedgerData                  { return entry.data }
