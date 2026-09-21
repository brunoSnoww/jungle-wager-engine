//go:build load

package load

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// The load report must state concurrency conflicts, and a field that always
// reads zero is indistinguishable from one that is never read.
//
// This generates the traffic it measures rather than relying on another test
// having run first: a CounterVec publishes no series at all until some label
// value is observed, so against a freshly started stack an absolute "> 0"
// assertion fails while claiming the reader is broken. The delta is the only
// honest measurement.
func TestCounterReadsWhatTheServicePublished(t *testing.T) {
	l := newLab(t)
	duplicatesBefore := l.counter("jungle_duplicates_total")
	conflictsBefore := l.counter("jungle_conflicts_total")

	w := l.openWallet("100.00")
	external := l.external(1)
	body := l.bet(w, 1, "1.00")

	post := func(payload []byte) int {
		req, err := http.NewRequest(http.MethodPost, l.target("/wagering/transactions"), strings.NewReader(string(payload)))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", l.provider.bearer())
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "provider-a:"+external)
		resp, err := l.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	if got := post(body); got != http.StatusCreated {
		t.Fatalf("first attempt status %d, want 201", got)
	}
	if got := post(body); got != http.StatusOK { // same identity, same payload
		t.Fatalf("replay status %d, want 200", got)
	}
	conflicting := []byte(strings.Replace(string(body), `"amount":"1.00"`, `"amount":"7.00"`, 1))
	if got := post(conflicting); got != http.StatusConflict { // same key, different payload
		t.Fatalf("conflicting replay status %d, want 409", got)
	}

	if delta := l.counter("jungle_duplicates_total") - duplicatesBefore; delta < 1 {
		t.Errorf("replay produced a duplicates delta of %.0f; the reader is not seeing the service's series", delta)
	}
	if delta := l.counter("jungle_conflicts_total") - conflictsBefore; delta < 1 {
		t.Errorf("conflict produced a conflicts delta of %.0f; the reader is not seeing the service's series", delta)
	}
	if got := l.counter(fmt.Sprintf("jungle_no_such_metric_%d_total", 1)); got != 0 {
		t.Fatalf("unknown metric read %.0f, want 0", got)
	}
}
