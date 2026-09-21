package domain

import (
	"encoding/json"
	"testing"
)

func TestBalanceChangedWireContract(t *testing.T) {
	event, err := NewWalletBalanceChanged(EventMeta{ID: EventID{6}, CorrelationID: "request", OccurredAt: testTime}, fixtureLedger(t), 2)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := envelope.Data[key]; !ok {
			t.Errorf("required event property absent: %s", key)
		}
	}
	if len(envelope.Data) != 7 {
		t.Fatalf("unexpected event contract: %s", raw)
	}
}
