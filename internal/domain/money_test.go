package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"
)

func money(t *testing.T, minor int64, currency string) Money {
	t.Helper()
	m, err := NewMoney(minor, currency)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var target *Error
	if !errors.As(err, &target) || target.Code != code {
		t.Fatalf("error = %v, want code %s", err, code)
	}
}

func TestParseMoneyExact(t *testing.T) {
	for _, tc := range []struct {
		input, canonical string
		minor            int64
	}{
		{"0", "0.00", 0}, {"0.0", "0.00", 0}, {"00.00", "0.00", 0},
		{"25", "25.00", 2500}, {"25.0", "25.00", 2500}, {"25.00", "25.00", 2500},
		{"00025.01", "25.01", 2501}, {"0.01", "0.01", 1},
		{"92233720368547758.07", "92233720368547758.07", math.MaxInt64},
	} {
		t.Run(tc.input, func(t *testing.T) {
			m, err := ParseMoney(tc.input, "BRL")
			if err != nil || m.Minor() != tc.minor || m.String() != tc.canonical {
				t.Fatalf("got %v %d %v", m, m.Minor(), err)
			}
			b, err := json.Marshal(m)
			if err != nil || string(b) != fmt.Sprintf(`{"amount":%q,"currency":"BRL"}`, tc.canonical) {
				t.Fatalf("JSON %s %v", b, err)
			}
		})
	}
}

func TestParseMoneyRejectsInvalid(t *testing.T) {
	for _, tc := range []struct{ input, code string }{
		{"", CodeInvalidAmount}, {"-0.00", CodeInvalidAmount}, {"-1", CodeInvalidAmount}, {"+1", CodeInvalidAmount},
		{"NaN", CodeInvalidAmount}, {"Infinity", CodeInvalidAmount}, {"1e2", CodeInvalidAmount}, {" 1", CodeInvalidAmount},
		{"1 ", CodeInvalidAmount}, {".10", CodeInvalidAmount}, {"1.", CodeInvalidAmount}, {"1.0.0", CodeInvalidAmount},
		{"1,00", CodeInvalidAmount}, {"１.00", CodeInvalidAmount}, {"1.001", CodeInvalidScale},
		{"92233720368547758.08", CodeOverflow}, {"92233720368547759", CodeOverflow},
	} {
		t.Run(tc.input, func(t *testing.T) { _, err := ParseMoney(tc.input, "BRL"); requireCode(t, err, tc.code) })
	}
	for _, currency := range []string{"", "brl", "BR", "XYZ", "JPY"} {
		_, err := ParseMoney("1", currency)
		requireCode(t, err, CodeInvalidCurrency)
	}
}

func TestMoneyArithmeticBoundaries(t *testing.T) {
	max, min, one, negativeOne, zero := money(t, math.MaxInt64, "BRL"), money(t, math.MinInt64, "BRL"), money(t, 1, "BRL"), money(t, -1, "BRL"), money(t, 0, "BRL")
	for _, tc := range []struct {
		name string
		fn   func() (Money, error)
	}{
		{"max+1", func() (Money, error) { return max.Add(one) }}, {"min-1", func() (Money, error) { return min.Add(negativeOne) }},
		{"max-(-1)", func() (Money, error) { return max.Subtract(negativeOne) }}, {"min-1 subtract", func() (Money, error) { return min.Subtract(one) }},
		{"negate min", min.Negate}, {"0-min", func() (Money, error) { return zero.Subtract(min) }},
	} {
		t.Run(tc.name, func(t *testing.T) { _, err := tc.fn(); requireCode(t, err, CodeOverflow) })
	}
	for _, tc := range []struct {
		left, right Money
		subtract    bool
		want        int64
	}{
		{max, negativeOne, false, math.MaxInt64 - 1}, {min, one, false, math.MinInt64 + 1},
		{min, min, true, 0}, {max, max, true, 0}, {negativeOne, min, true, math.MaxInt64},
		{zero, max, true, -math.MaxInt64}, {one, negativeOne, true, 2},
	} {
		var got Money
		var err error
		if tc.subtract {
			got, err = tc.left.Subtract(tc.right)
		} else {
			got, err = tc.left.Add(tc.right)
		}
		if err != nil || got.Minor() != tc.want {
			t.Fatalf("arithmetic got %d %v want %d", got.Minor(), err, tc.want)
		}
	}
	if min.String() != "-92233720368547758.08" || negativeOne.String() != "-0.01" {
		t.Fatal("signed formatting")
	}
	neg, err := one.Negate()
	if err != nil || neg.Minor() != -1 {
		t.Fatal(neg, err)
	}
	if one.Minor() != 1 {
		t.Fatal("money mutated")
	}
	for _, pair := range []struct {
		a, b Money
		want int
	}{{one, zero, 1}, {min, max, -1}, {max, max, 0}} {
		got, err := pair.a.Compare(pair.b)
		if err != nil || got != pair.want {
			t.Fatal(got, err)
		}
	}
}

func TestMoneyInvalidAndCurrencyMismatch(t *testing.T) {
	brl, usd := money(t, 100, "BRL"), money(t, 100, "USD")
	_, err := brl.Add(usd)
	requireCode(t, err, CodeCurrencyMismatch)
	_, err = brl.Subtract(usd)
	requireCode(t, err, CodeCurrencyMismatch)
	_, err = brl.Compare(usd)
	requireCode(t, err, CodeCurrencyMismatch)
	for _, fn := range []func() (Money, error){func() (Money, error) { return Money{}.Add(brl) }, func() (Money, error) { return brl.Subtract(Money{}) }, func() (Money, error) { return Money{}.Negate() }} {
		_, err = fn()
		requireCode(t, err, CodeInvalidMoney)
	}
	_, err = Money{}.Compare(brl)
	requireCode(t, err, CodeInvalidMoney)
	_, err = json.Marshal(Money{})
	requireCode(t, err, CodeInvalidMoney)
	wrapped := fmt.Errorf("adapter: %w", invalid(CodeInsufficientFunds, "no funds"))
	requireCode(t, wrapped, CodeInsufficientFunds)
}
