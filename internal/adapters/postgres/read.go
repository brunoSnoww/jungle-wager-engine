package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"jungle/internal/application"
	"jungle/internal/domain"
	"math/big"
	"time"
)

type ledgerCursor struct {
	Version  int       `json:"v"`
	WalletID string    `json:"w"`
	ID       string    `json:"i"`
	At       time.Time `json:"t"`
}

func (s *Store) Ledger(ctx context.Context, id, cursor string, limit int) (application.LedgerPage, error) {
	if _, err := domain.ParseID(id); err != nil {
		return application.LedgerPage{}, err
	}
	if limit < 1 || limit > 100 {
		return application.LedgerPage{}, &application.Error{Code: "INVALID_INPUT"}
	}
	if _, err := s.GetWallet(ctx, id); err != nil {
		return application.LedgerPage{}, err
	}
	var at any
	var entryID any
	if cursor != "" {
		if len(cursor) > 512 {
			return application.LedgerPage{}, &application.Error{Code: "INVALID_CURSOR"}
		}
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return application.LedgerPage{}, &application.Error{Code: "INVALID_CURSOR"}
		}
		var c ledgerCursor
		if err = json.Unmarshal(raw, &c); err != nil || c.Version != 1 || c.WalletID != id || c.At.IsZero() {
			return application.LedgerPage{}, &application.Error{Code: "INVALID_CURSOR"}
		}
		if _, err = domain.ParseID(c.ID); err != nil {
			return application.LedgerPage{}, &application.Error{Code: "INVALID_CURSOR"}
		}
		at, entryID = c.At, c.ID
	}
	rows, err := s.pool.Query(ctx, `SELECT id::text,transaction_id::text,direction,amount_minor,currency,balance_before_minor,balance_after_minor,created_at FROM wallet_ledger_entry WHERE wallet_id=$1 AND ($2::timestamptz IS NULL OR (created_at,id)<($2,$3::uuid)) ORDER BY created_at DESC,id DESC LIMIT $4`, id, at, entryID, limit+1)
	if err != nil {
		return application.LedgerPage{}, dbError(err)
	}
	defer rows.Close()
	page := application.LedgerPage{Entries: make([]application.LedgerEntry, 0, limit)}
	for rows.Next() {
		var e application.LedgerEntry
		var amount, before, after int64
		var currency string
		if err = rows.Scan(&e.ID, &e.TransactionID, &e.Direction, &amount, &currency, &before, &after, &e.CreatedAt); err != nil {
			return page, dbError(err)
		}
		e.WalletID = id
		e.Money, err = domain.NewMoney(amount, currency)
		if err != nil {
			return page, err
		}
		e.BalanceBefore, err = domain.NewMoney(before, currency)
		if err != nil {
			return page, err
		}
		e.BalanceAfter, err = domain.NewMoney(after, currency)
		if err != nil {
			return page, err
		}
		if len(page.Entries) == limit {
			last := page.Entries[limit-1]
			raw, e := json.Marshal(ledgerCursor{Version: 1, WalletID: id, ID: last.ID, At: last.CreatedAt})
			if e != nil {
				return page, e
			}
			page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
			break
		}
		page.Entries = append(page.Entries, e)
	}
	return page, rows.Err()
}

func (s *Store) Reconcile(ctx context.Context, id string) (application.Reconciliation, error) {
	if _, err := domain.ParseID(id); err != nil {
		return application.Reconciliation{}, err
	}
	var r application.Reconciliation
	var stored int64
	var currency, total string
	// One statement, one MVCC snapshot. Individual historical credit/debit totals
	// may exceed int64, so PostgreSQL sums NUMERIC and only the net is range checked.
	err := s.pool.QueryRow(ctx, `SELECT w.id::text,w.balance_minor,w.currency,COALESCE(l.total,0)::text,COALESCE(l.count,0)
 FROM wallet w LEFT JOIN LATERAL (SELECT sum(CASE direction WHEN 'CREDIT' THEN amount_minor::numeric ELSE -amount_minor::numeric END) total,count(*) count FROM wallet_ledger_entry WHERE wallet_id=w.id) l ON true WHERE w.id=$1`, id).Scan(&r.WalletID, &stored, &currency, &total, &r.CheckedEntries)
	if err != nil {
		return r, dbError(err)
	}
	exact, ok := new(big.Int).SetString(total, 10)
	if !ok || !exact.IsInt64() {
		return r, &application.Error{Code: "RECONCILIATION_OVERFLOW"}
	}
	r.StoredBalance, err = domain.NewMoney(stored, currency)
	if err != nil {
		return r, err
	}
	r.CalculatedBalance, err = domain.NewMoney(exact.Int64(), currency)
	if err != nil {
		return r, err
	}
	r.Difference, err = r.StoredBalance.Subtract(r.CalculatedBalance)
	if err != nil {
		return r, err
	}
	r.Consistent = r.Difference.Minor() == 0
	return r, nil
}
