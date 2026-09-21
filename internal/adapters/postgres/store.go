// Package postgres implements the atomic application boundary. Every write in
// ProcessWager uses the same pgx.Tx. No repository opens an independent commit.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"jungle/internal/adapters/postgres/sqlc"
	"jungle/internal/application"
	"jungle/internal/domain"
)

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

type DBTX interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func dbError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return &application.Error{Code: "NOT_FOUND"}
	}
	var e *pgconn.PgError
	if errors.As(err, &e) && e.Code == "23505" && e.ConstraintName == "wallet_player_currency_key" {
		return &application.Error{Code: "WALLET_ALREADY_EXISTS"}
	}
	return fmt.Errorf("postgres operation: %w", err)
}
func allocateID(ctx context.Context, q DBTX) (domain.ID, error) {
	value, err := sqlc.New(q).AllocateID(ctx)
	if err != nil {
		return domain.ID{}, err
	}
	return domain.ParseID(value)
}
func optional(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func walletView(w domain.WalletData) application.WalletView {
	return application.WalletView{ID: w.ID.String(), PlayerID: w.PlayerID.String(), Balance: w.Balance, Version: w.Version, CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt}
}

const walletColumns = `id::text,player_id::text,balance_minor,currency,version,created_at,updated_at`

func scanWallet(row pgx.Row) (domain.WalletData, error) {
	var w domain.WalletData
	var id, player, currency string
	var minor int64
	if err := row.Scan(&id, &player, &minor, &currency, &w.Version, &w.CreatedAt, &w.UpdatedAt); err != nil {
		return w, err
	}
	uid, err := domain.ParseID(id)
	if err != nil {
		return w, err
	}
	pid, err := domain.ParseID(player)
	if err != nil {
		return w, err
	}
	w.ID = domain.WalletID(uid)
	w.PlayerID = domain.PlayerID(pid)
	w.Balance, err = domain.NewMoney(minor, currency)
	if err != nil {
		return w, err
	}
	_, err = domain.RehydrateWallet(w)
	return w, err
}
func (s *Store) GetWallet(ctx context.Context, id string) (application.WalletView, error) {
	if _, err := domain.ParseID(id); err != nil {
		return application.WalletView{}, err
	}
	w, err := scanWallet(s.pool.QueryRow(ctx, "SELECT "+walletColumns+" FROM wallet WHERE id=$1", id))
	if err != nil {
		return application.WalletView{}, dbError(err)
	}
	return walletView(w), nil
}

// lockWallet is the sole financial serialization point. Its lock is always
// acquired before inserting or locking any wager, including reference workers.
func lockWallet(ctx context.Context, tx pgx.Tx, id string) (domain.WalletData, error) {
	uid, err := domain.ParseID(id)
	if err != nil {
		return domain.WalletData{}, err
	}
	r, err := sqlc.New(tx).GetWalletForUpdate(ctx, pgtype.UUID{Bytes: [16]byte(uid), Valid: true})
	if err != nil {
		return domain.WalletData{}, err
	}
	pid, err := domain.ParseID(r.PlayerID)
	if err != nil {
		return domain.WalletData{}, err
	}
	balance, err := domain.NewMoney(r.BalanceMinor, r.Currency)
	if err != nil {
		return domain.WalletData{}, err
	}
	w := domain.WalletData{ID: domain.WalletID(uid), PlayerID: domain.PlayerID(pid), Balance: balance, Version: r.Version, CreatedAt: r.CreatedAt.Time, UpdatedAt: r.UpdatedAt.Time}
	_, err = domain.RehydrateWallet(w)
	return w, err
}

func (s *Store) CreateWallet(ctx context.Context, c application.CreateWalletCommand) (application.WalletView, error) {
	pid, err := domain.ParseID(c.PlayerID)
	if err != nil {
		return application.WalletView{}, err
	}
	if _, err = domain.NewMoney(c.InitialBalance.Minor(), c.InitialBalance.Currency()); err != nil {
		return application.WalletView{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return application.WalletView{}, dbError(err)
	}
	defer tx.Rollback(ctx)
	id, err := allocateID(ctx, tx)
	if err != nil {
		return application.WalletView{}, dbError(err)
	}
	now := time.Now().UTC()
	w, err := domain.NewWallet(domain.WalletData{ID: domain.WalletID(id), PlayerID: domain.PlayerID(pid), Balance: c.InitialBalance, Version: 1, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		return application.WalletView{}, err
	}
	var persisted string
	err = tx.QueryRow(ctx, `INSERT INTO wallet(id,player_id,currency,balance_minor,version,created_at,updated_at) VALUES($1,$2,$3,$4,1,$5,$5) RETURNING id::text`, id.String(), pid.String(), c.InitialBalance.Currency(), c.InitialBalance.Minor(), now).Scan(&persisted)
	if err != nil {
		return application.WalletView{}, dbError(err)
	}
	if c.InitialBalance.Minor() > 0 {
		tid, e := allocateID(ctx, tx)
		if e != nil {
			return application.WalletView{}, dbError(e)
		}
		wg, e := domain.NewOpening(domain.WagerData{ID: domain.WagerTransactionID(tid), WalletID: domain.WalletID(id), PlayerID: domain.PlayerID(pid), Kind: domain.KindOpening, Money: c.InitialBalance, Status: domain.StatusPending, CreatedAt: now, UpdatedAt: now})
		if e != nil {
			return application.WalletView{}, e
		}
		if e = wg.Process(c.InitialBalance, 1, domain.WagerTransactionID{}, now); e != nil {
			return application.WalletView{}, e
		}
		d := wg.Snapshot()
		corr := c.CorrelationID
		if corr == "" {
			corr = id.String()
		}
		_, e = tx.Exec(ctx, `INSERT INTO wager_transaction(id,origin,wallet_id,player_id,kind,amount_minor,currency,status,result_balance_minor,result_currency,result_wallet_version,correlation_id,created_at,updated_at,completed_at) VALUES($1,'INTERNAL',$2,$3,'OPENING',$4,$5,'PROCESSED',$4,$5,1,$6,$7,$7,$7)`, tid.String(), id.String(), pid.String(), c.InitialBalance.Minor(), c.InitialBalance.Currency(), corr, now)
		if e != nil {
			return application.WalletView{}, dbError(e)
		}
		zero, e := domain.Zero(c.InitialBalance.Currency())
		if e != nil {
			return application.WalletView{}, e
		}
		entry, e := insertLedger(ctx, tx, d, domain.DirectionCredit, zero, c.InitialBalance, now)
		if e != nil {
			return application.WalletView{}, dbError(e)
		}
		if e = writeEvents(ctx, tx, d, w.Snapshot(), &entry, corr, "", now); e != nil {
			return application.WalletView{}, dbError(e)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return application.WalletView{}, dbError(err)
	}
	return walletView(w.Snapshot()), nil
}

const wagerColumns = `t.id::text,t.wallet_id::text,t.player_id::text,COALESCE(t.provider_id,''),COALESCE(t.external_transaction_id,''),COALESCE(t.idempotency_key,''),COALESCE(encode(t.payload_hash,'hex'),''),COALESCE(t.round_id,''),COALESCE(t.game_id,''),t.kind,t.amount_minor,t.currency,COALESCE(t.reference_external_transaction_id,''),COALESCE(t.reference_transaction_id::text,''),t.status,COALESCE(t.failure_code,''),t.result_balance_minor,t.result_currency,t.result_wallet_version,t.created_at,t.updated_at,t.completed_at`

func scanWager(row pgx.Row) (domain.WagerData, error) {
	var d domain.WagerData
	var id, wall, player, ref, currency string
	var amount int64
	var balance, version *int64
	var resultCurrency *string
	err := row.Scan(&id, &wall, &player, &d.ProviderID, &d.ExternalTransactionID, &d.IdempotencyKey, &d.PayloadHash, &d.RoundID, &d.GameID, &d.Kind, &amount, &currency, &d.ReferenceExternalTransactionID, &ref, &d.Status, &d.FailureCode, &balance, &resultCurrency, &version, &d.CreatedAt, &d.UpdatedAt, &d.CompletedAt)
	if err != nil {
		return d, err
	}
	idv, err := domain.ParseID(id)
	if err != nil {
		return d, err
	}
	wv, err := domain.ParseID(wall)
	if err != nil {
		return d, err
	}
	pv, err := domain.ParseID(player)
	if err != nil {
		return d, err
	}
	d.ID = domain.WagerTransactionID(idv)
	d.WalletID = domain.WalletID(wv)
	d.PlayerID = domain.PlayerID(pv)
	if ref != "" {
		r, e := domain.ParseID(ref)
		if e != nil {
			return d, e
		}
		d.ReferenceID = domain.WagerTransactionID(r)
	}
	d.Money, err = domain.NewMoney(amount, currency)
	if err != nil {
		return d, err
	}
	if balance != nil && version != nil && resultCurrency != nil {
		d.ResultBalance, err = domain.NewMoney(*balance, *resultCurrency)
		if err != nil {
			return d, err
		}
		d.ResultVersion = *version
	}
	_, err = domain.RehydrateWager(d)
	return d, err
}
func resultOf(d domain.WagerData, w domain.WalletData, replay bool) application.Result {
	balance, version := d.ResultBalance, d.ResultVersion
	if !d.Status.Terminal() {
		balance, version = w.Balance, w.Version
	}
	return application.Result{TransactionID: d.ID.String(), Status: string(d.Status), Balance: balance, WalletVersion: version, FailureCode: d.FailureCode, IdempotentReplay: replay}
}
func (s *Store) getTransaction(ctx context.Context, where string, args ...any) (application.Result, error) {
	d, err := scanWager(s.pool.QueryRow(ctx, "SELECT "+wagerColumns+" FROM wager_transaction t WHERE "+where, args...))
	if err != nil {
		return application.Result{}, dbError(err)
	}
	w, err := scanWallet(s.pool.QueryRow(ctx, "SELECT "+walletColumns+" FROM wallet WHERE id=$1", d.WalletID.String()))
	if err != nil {
		return application.Result{}, dbError(err)
	}
	return resultOf(d, w, false), nil
}
func (s *Store) GetTransaction(ctx context.Context, provider, id string) (application.Result, error) {
	if _, err := domain.ParseID(id); err != nil {
		return application.Result{}, err
	}
	return s.getTransaction(ctx, "t.provider_id=$1 AND t.id=$2", provider, id)
}
func (s *Store) GetExternalTransaction(ctx context.Context, provider, external string) (application.Result, error) {
	return s.getTransaction(ctx, "t.provider_id=$1 AND t.external_transaction_id=$2", provider, external)
}
