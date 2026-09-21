package domain

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

var testTime = time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

func fixtureWallet(t *testing.T) WalletData {
	t.Helper()
	return WalletData{ID: WalletID{1}, PlayerID: PlayerID{2}, Balance: money(t, 10000, "BRL"), Version: 1, CreatedAt: testTime, UpdatedAt: testTime}
}

func fixtureWager(t *testing.T, kind Kind, minor int64) WagerData {
	t.Helper()
	return WagerData{ID: WagerTransactionID{3}, WalletID: WalletID{1}, PlayerID: PlayerID{2}, ProviderID: "provider-a", ExternalTransactionID: "operation", IdempotencyKey: "key", PayloadHash: strings.Repeat("a", 64), RoundID: "round", GameID: "game", Kind: kind, Money: money(t, minor, "BRL"), Status: StatusPending, CreatedAt: testTime, UpdatedAt: testTime}
}

func fixtureReference(t *testing.T, kind Kind, minor int64, status Status) WagerData {
	t.Helper()
	d := fixtureWager(t, kind, minor)
	d.ID = WagerTransactionID{4}
	d.ExternalTransactionID = "original"
	d.Status = status
	if status.Terminal() {
		at := testTime
		d.CompletedAt = &at
		d.ResultBalance = money(t, 10000, "BRL")
		d.ResultVersion = 1
	}
	if status == StatusRejected || status == StatusFailed {
		d.FailureCode = CodeInsufficientFunds
	}
	return d
}

func TestIDsRoundTrip(t *testing.T) {
	const text = "0192F28F-5DC0-7D58-BDB2-814AD6A0F4A1"
	id, err := ParseID(text)
	if err != nil || id.String() != strings.ToLower(text) {
		t.Fatal(id, err)
	}
	for _, invalidID := range []string{"", strings.Repeat("0", 36), "00000000-0000-0000-0000-000000000000", "0192f28f_5dc0-7d58-bdb2-814ad6a0f4a1", "0192f28f-5dc0-7d58-bdb2-814ad6a0f4ax"} {
		_, err := ParseID(invalidID)
		requireCode(t, err, CodeInvalidID)
	}
	for _, value := range []any{id, WalletID(id), PlayerID(id), WagerTransactionID(id), LedgerEntryID(id), EventID(id)} {
		b, err := json.Marshal(value)
		if err != nil || string(b) != `"`+strings.ToLower(text)+`"` {
			t.Fatalf("ID encoding %s %v", b, err)
		}
	}
}

func TestWalletOperationsAndRehydration(t *testing.T) {
	original := fixtureWallet(t)
	w, err := NewWallet(original)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Debit(money(t, 8000, "BRL"), testTime.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if w.Snapshot().Balance.Minor() != 2000 || w.Snapshot().Version != 2 {
		t.Fatal(w.Snapshot())
	}
	before := w.Snapshot()
	requireCode(t, w.Debit(money(t, 8000, "BRL"), testTime.Add(2*time.Second)), CodeInsufficientFunds)
	if w.Snapshot() != before {
		t.Fatal("failed debit mutated state")
	}
	requireCode(t, w.Credit(money(t, 1, "USD"), testTime.Add(time.Second)), CodeCurrencyMismatch)
	requireCode(t, w.Credit(money(t, -1, "BRL"), testTime.Add(time.Second)), CodeInvalidAmount)
	if err = w.Credit(money(t, 0, "BRL"), testTime.Add(time.Second)); err != nil || w.Snapshot() != before {
		t.Fatal("zero changed wallet", err)
	}
	if err = w.Credit(money(t, 100, "BRL"), testTime.Add(2*time.Second)); err != nil || w.Snapshot().Version != 3 {
		t.Fatal(err)
	}
	restored, err := RehydrateWallet(w.Snapshot())
	if err != nil || restored.Snapshot() != w.Snapshot() {
		t.Fatal("rehydration changed wallet", err)
	}
	_, err = NewWallet(w.Snapshot())
	requireCode(t, err, CodeInvalidWallet)
	requireCode(t, w.Debit(money(t, 1, "BRL"), testTime), CodeInvalidWallet)
}

func TestWalletInvalidAndOverflow(t *testing.T) {
	for _, mutate := range []func(*WalletData){func(d *WalletData) { d.ID = WalletID{} }, func(d *WalletData) { d.PlayerID = PlayerID{} }, func(d *WalletData) { d.Balance = Money{} }, func(d *WalletData) { d.Balance = money(t, -1, "BRL") }, func(d *WalletData) { d.Version = 0 }, func(d *WalletData) { d.CreatedAt = time.Time{} }, func(d *WalletData) { d.UpdatedAt = testTime.Add(-time.Second) }} {
		d := fixtureWallet(t)
		mutate(&d)
		_, err := RehydrateWallet(d)
		requireCode(t, err, CodeInvalidWallet)
	}
	d := fixtureWallet(t)
	d.Balance = money(t, math.MaxInt64, "BRL")
	w, _ := NewWallet(d)
	requireCode(t, w.Credit(money(t, 1, "BRL"), testTime), CodeOverflow)
	if w.Snapshot() != d {
		t.Fatal("overflow mutated wallet")
	}
	d = fixtureWallet(t)
	d.Version = math.MaxInt64
	w, _ = RehydrateWallet(d)
	requireCode(t, w.Debit(money(t, 1, "BRL"), testTime), CodeOverflow)
	var zero Wallet
	requireCode(t, zero.Credit(money(t, 1, "BRL"), testTime), CodeInvalidWallet)
	var absent *Wallet
	requireCode(t, absent.Debit(money(t, 1, "BRL"), testTime), CodeInvalidWallet)
}

func TestWagerStateMachine(t *testing.T) {
	for _, start := range []Status{StatusPending, StatusPendingReference} {
		for _, end := range []Status{StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed} {
			t.Run(string(start)+"->"+string(end), func(t *testing.T) {
				d := fixtureWager(t, KindREFUND, 100)
				d.ReferenceExternalTransactionID = "original"
				d.Status = start
				w, err := RehydrateWager(d)
				if err != nil {
					t.Fatal(err)
				}
				switch end {
				case StatusPendingReference:
					err = w.PendingReference(testTime)
				case StatusProcessed:
					err = w.Process(money(t, 10100, "BRL"), 2, WagerTransactionID{4}, testTime)
				case StatusRejected:
					err = w.Reject(CodeReferenceMismatch, money(t, 10000, "BRL"), 1, testTime)
				case StatusFailed:
					err = w.Fail("PERMANENT_STORAGE_ERROR", money(t, 0, "BRL"), 3, testTime)
				}
				if err != nil || w.Snapshot().Status != end {
					t.Fatal(w.Snapshot(), err)
				}
				if end.Terminal() {
					if w.Snapshot().CompletedAt == nil {
						t.Fatal("terminal missing time")
					}
					for _, fn := range []func() error{func() error { return w.PendingReference(testTime) }, func() error { return w.Process(money(t, 0, "BRL"), 3, WagerTransactionID{4}, testTime) }, func() error { return w.Reject("OTHER", money(t, 0, "BRL"), 3, testTime) }, func() error { return w.Fail("OTHER", money(t, 0, "BRL"), 3, testTime) }} {
						requireCode(t, fn(), CodeInvalidState)
					}
				}
			})
		}
	}
}

func TestWagerCreationAndImmutableSnapshots(t *testing.T) {
	d := fixtureWager(t, KindBET, 100)
	w, err := NewWager(d)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Process(money(t, 9900, "BRL"), 2, WagerTransactionID{}, testTime); err != nil {
		t.Fatal(err)
	}
	s := w.Snapshot()
	*s.CompletedAt = time.Time{}
	if w.Snapshot().CompletedAt.IsZero() {
		t.Fatal("snapshot pointer mutated entity")
	}
	s = w.Snapshot()
	r, err := RehydrateWager(s)
	if err != nil {
		t.Fatal(err)
	}
	*s.CompletedAt = time.Time{}
	if r.Snapshot().CompletedAt.IsZero() {
		t.Fatal("input pointer retained")
	}
	d = fixtureWager(t, KindOpening, 100)
	_, err = NewWager(d)
	requireCode(t, err, CodeInvalidWager)
	d.ProviderID, d.ExternalTransactionID, d.IdempotencyKey, d.PayloadHash, d.RoundID, d.GameID = "", "", "", "", "", ""
	w, err = NewOpening(d)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Process(money(t, 100, "BRL"), 1, WagerTransactionID{}, testTime); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*WagerData){func(d *WagerData) { d.ID = WagerTransactionID{} }, func(d *WagerData) { d.ProviderID = " " }, func(d *WagerData) { d.Kind = Kind("UNKNOWN") }, func(d *WagerData) { d.PayloadHash = "invalid" }, func(d *WagerData) { d.Money = Money{} }} {
		d := fixtureWager(t, KindBET, 100)
		mutate(&d)
		if _, err := NewWager(d); err == nil {
			t.Fatal("accepted invalid wager")
		}
	}
	w, _ = NewWager(fixtureWager(t, KindBET, 100))
	requireCode(t, w.PendingReference(testTime), CodeInvalidState)
	w, _ = NewWager(fixtureWager(t, KindBET, 0))
	requireCode(t, w.Process(money(t, 10000, "BRL"), 1, WagerTransactionID{}, testTime), CodeInvalidAmountForKind)
}

func TestEvaluateFinancialKinds(t *testing.T) {
	for _, tc := range []struct {
		kind      Kind
		amount    int64
		direction Direction
		failure   string
	}{
		{KindBET, 8000, DirectionDebit, ""}, {KindBET, 10001, DirectionNone, CodeInsufficientFunds}, {KindBET, 0, DirectionNone, CodeInvalidAmountForKind},
		{KindWIN, 100, DirectionCredit, ""}, {KindWIN, 0, DirectionNone, CodeInvalidAmountForKind}, {KindLOSS, 0, DirectionNone, ""}, {KindLOSS, 1, DirectionNone, CodeInvalidAmountForKind},
	} {
		t.Run(string(tc.kind)+money(t, tc.amount, "BRL").String(), func(t *testing.T) {
			decision, err := Evaluate(fixtureWallet(t), fixtureWager(t, tc.kind, tc.amount), nil, false)
			if err != nil || decision.Direction != tc.direction || decision.FailureCode != tc.failure || decision.Pending {
				t.Fatal(decision, err)
			}
		})
	}
	d := fixtureWager(t, KindBET, 100)
	d.Money = money(t, 100, "USD")
	decision, err := Evaluate(fixtureWallet(t), d, nil, false)
	if err != nil || decision.FailureCode != CodeCurrencyMismatch {
		t.Fatal(decision, err)
	}
	wallet := fixtureWallet(t)
	wallet.Balance = money(t, math.MaxInt64, "BRL")
	decision, err = Evaluate(wallet, fixtureWager(t, KindWIN, 1), nil, false)
	if err != nil || decision.FailureCode != CodeOverflow {
		t.Fatal(decision, err)
	}
}

func TestEvaluateReferences(t *testing.T) {
	for _, tc := range []struct {
		kind, refKind Kind
		direction     Direction
		failure       string
	}{
		{KindREFUND, KindBET, DirectionCredit, ""}, {KindROLLBACK, KindBET, DirectionCredit, ""}, {KindROLLBACK, KindWIN, DirectionDebit, ""}, {KindROLLBACK, KindREFUND, DirectionDebit, ""},
		{KindREFUND, KindWIN, DirectionNone, CodeReferenceMismatch}, {KindROLLBACK, KindROLLBACK, DirectionNone, CodeReferenceMismatch}, {KindWIN, KindBET, DirectionCredit, ""},
	} {
		t.Run(string(tc.kind)+"("+string(tc.refKind)+")", func(t *testing.T) {
			w := fixtureWager(t, tc.kind, 100)
			w.ReferenceExternalTransactionID = "original"
			ref := fixtureReference(t, tc.refKind, 100, StatusProcessed)
			decision, err := Evaluate(fixtureWallet(t), w, &ref, false)
			if err != nil || decision.Direction != tc.direction || decision.FailureCode != tc.failure {
				t.Fatal(decision, err)
			}
		})
	}
	w := fixtureWager(t, KindROLLBACK, 100)
	w.ReferenceExternalTransactionID = "original"
	ref := fixtureReference(t, KindWIN, 100, StatusProcessed)
	for _, mutate := range []func(*WagerData){func(d *WagerData) { d.ProviderID = "other" }, func(d *WagerData) { d.PlayerID = PlayerID{9} }, func(d *WagerData) { d.WalletID = WalletID{9} }, func(d *WagerData) { d.RoundID = "other" }, func(d *WagerData) { d.Money = money(t, 100, "USD") }, func(d *WagerData) { d.Money = money(t, 99, "BRL") }, func(d *WagerData) { d.ExternalTransactionID = "other" }, func(d *WagerData) { d.ID = w.ID }} {
		r := ref
		mutate(&r)
		decision, err := Evaluate(fixtureWallet(t), w, &r, false)
		if err != nil || decision.FailureCode != CodeReferenceMismatch {
			t.Fatal(decision, err)
		}
	}
	for _, status := range []Status{StatusPending, StatusPendingReference, StatusRejected, StatusFailed} {
		r := ref
		r.Status = status
		decision, err := Evaluate(fixtureWallet(t), w, &r, false)
		if err != nil {
			t.Fatal(err)
		}
		if status == StatusPending || status == StatusPendingReference {
			if !decision.Pending {
				t.Fatal(decision)
			}
		} else if decision.FailureCode != CodeReferenceNotProcessed {
			t.Fatal(decision)
		}
	}
	decision, err := Evaluate(fixtureWallet(t), w, nil, false)
	if err != nil || !decision.Pending {
		t.Fatal(decision, err)
	}
	decision, err = Evaluate(fixtureWallet(t), w, &ref, true)
	if err != nil || decision.FailureCode != CodeReferenceAlreadyReversed {
		t.Fatal(decision, err)
	}
	wallet := fixtureWallet(t)
	wallet.Balance = money(t, 0, "BRL")
	decision, err = Evaluate(wallet, w, &ref, false)
	if err != nil || decision.FailureCode != CodeInsufficientFundsForReversal {
		t.Fatal(decision, err)
	}
	w.Kind = KindWIN
	ref.Kind = KindBET
	ref.Money = money(t, 8000, "BRL")
	decision, err = Evaluate(fixtureWallet(t), w, &ref, true)
	if err != nil || decision.FailureCode != "" || decision.Direction != DirectionCredit {
		t.Fatal("WIN context should not consume reversal", decision, err)
	}
}

func fixtureLedger(t *testing.T) LedgerData {
	return LedgerData{ID: LedgerEntryID{5}, WalletID: WalletID{1}, TransactionID: WagerTransactionID{3}, Direction: DirectionDebit, Money: money(t, 8000, "BRL"), BalanceBefore: money(t, 10000, "BRL"), BalanceAfter: money(t, 2000, "BRL"), CreatedAt: testTime}
}

func TestLedgerInvariant(t *testing.T) {
	d := fixtureLedger(t)
	entry, err := NewLedgerEntry(d)
	if err != nil || entry.Snapshot() != d {
		t.Fatal(entry, err)
	}
	for _, mutate := range []func(*LedgerData){func(d *LedgerData) { d.ID = LedgerEntryID{} }, func(d *LedgerData) { d.Direction = DirectionNone }, func(d *LedgerData) { d.Money = money(t, 0, "BRL") }, func(d *LedgerData) { d.Money = money(t, -1, "BRL") }, func(d *LedgerData) { d.BalanceAfter = money(t, 2001, "BRL") }, func(d *LedgerData) { d.BalanceBefore = money(t, -1, "BRL") }, func(d *LedgerData) { d.BalanceAfter = money(t, 2000, "USD") }} {
		data := d
		mutate(&data)
		if _, err = NewLedgerEntry(data); err == nil {
			t.Fatal("invalid ledger accepted")
		}
	}
	d.Direction = DirectionCredit
	d.BalanceAfter = money(t, 18000, "BRL")
	if _, err = NewLedgerEntry(d); err != nil {
		t.Fatal(err)
	}
}

func TestTypedEventsAndSnapshot(t *testing.T) {
	w, _ := NewWager(fixtureWager(t, KindBET, 8000))
	if err := w.Process(money(t, 2000, "BRL"), 2, WagerTransactionID{}, testTime); err != nil {
		t.Fatal(err)
	}
	meta := EventMeta{ID: EventID{6}, CorrelationID: "request", OccurredAt: testTime.In(time.FixedZone("offset", 3600))}
	event, err := NewWagerProcessed(meta, w.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		EventID    string
		EventType  string
		Version    int
		OccurredAt string
		Data       struct{ Money struct{ Amount string } }
	}
	if err = json.Unmarshal(data, &envelope); err != nil || envelope.EventID != meta.ID.String() || envelope.EventType != EventWagerProcessed || envelope.Version != 1 || !strings.HasSuffix(envelope.OccurredAt, "Z") || envelope.Data.Money.Amount != "80.00" {
		t.Fatal(string(data), err)
	}
	copyJSON, _ := event.MarshalJSON()
	copyJSON[0] = 'X'
	again, _ := event.MarshalJSON()
	if !bytes.Equal(data, again) {
		t.Fatal("caller mutated event")
	}
	if _, err = NewWagerRejected(meta, w.Snapshot()); err == nil {
		t.Fatal("wrong state event accepted")
	}
	changed, err := NewWalletBalanceChanged(meta, fixtureLedger(t), 2)
	if err != nil || changed.Type() != EventWalletBalanceChanged || changed.Version() != 1 {
		t.Fatal(changed, err)
	}
	if _, err = NewWalletBalanceChanged(meta, fixtureLedger(t), 0); err == nil {
		t.Fatal("zero version accepted")
	}
	if _, err = (Event{}).MarshalJSON(); err == nil {
		t.Fatal("zero event accepted")
	}
	w, _ = NewWager(fixtureWager(t, KindBET, 8000))
	_ = w.Reject(CodeInsufficientFunds, money(t, 0, "BRL"), 1, testTime)
	if _, err = NewWagerRejected(meta, w.Snapshot()); err != nil {
		t.Fatal(err)
	}
	d := fixtureWager(t, KindREFUND, 100)
	d.ReferenceExternalTransactionID = "original"
	w, _ = NewWager(d)
	_ = w.PendingReference(testTime)
	if _, err = NewWagerPendingReference(meta, w.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_ = w.Fail("PERMANENT_STORAGE_ERROR", money(t, 0, "BRL"), 3, testTime)
	if _, err = NewWagerFailed(meta, w.Snapshot()); err != nil {
		t.Fatal(err)
	}
}
