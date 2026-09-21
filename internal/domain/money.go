package domain

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

// Money stores signed minor units at a fixed scale of two. External amounts are
// nonnegative; signed construction is reserved for exact internal differences.
// Supported ISO 4217 currencies are BRL, USD and EUR, all with two minor digits.
type Money struct {
	minor    int64
	currency string
}

func validCurrency(currency string) bool {
	return currency == "BRL" || currency == "USD" || currency == "EUR"
}

func NewMoney(minor int64, currency string) (Money, error) {
	if !validCurrency(currency) {
		return Money{}, invalid(CodeInvalidCurrency, "unsupported ISO 4217 currency")
	}
	return Money{minor: minor, currency: currency}, nil
}

func Zero(currency string) (Money, error) { return NewMoney(0, currency) }

// ParseMoney accepts 25, 25.0 and 25.00 and normalizes each to 25.00.
// It never rounds, truncates or uses floating point.
func ParseMoney(amount, currency string) (Money, error) {
	if !validCurrency(currency) {
		return Money{}, invalid(CodeInvalidCurrency, "unsupported ISO 4217 currency")
	}
	if amount == "" {
		return Money{}, invalid(CodeInvalidAmount, "amount is required")
	}
	parts := strings.Split(amount, ".")
	if len(parts) > 2 || parts[0] == "" {
		return Money{}, invalid(CodeInvalidAmount, "expected unsigned decimal amount")
	}
	for _, part := range parts {
		if part == "" {
			return Money{}, invalid(CodeInvalidAmount, "expected decimal digits")
		}
		for _, digit := range part {
			if digit < '0' || digit > '9' {
				return Money{}, invalid(CodeInvalidAmount, "expected unsigned decimal amount")
			}
		}
	}
	fraction := "00"
	if len(parts) == 2 {
		if len(parts[1]) > 2 {
			return Money{}, invalid(CodeInvalidScale, "at most two fractional digits allowed")
		}
		fraction = parts[1]
		if len(fraction) == 1 {
			fraction += "0"
		}
	}
	// Parse directly as minor units so MaxInt64, whose whole component can be
	// multiplied safely but whose remainder may overflow, is checked as a whole.
	minor, err := strconv.ParseInt(strings.TrimLeft(parts[0]+fraction, "0"), 10, 64)
	if strings.TrimLeft(parts[0]+fraction, "0") == "" {
		return Zero(currency)
	}
	if err != nil {
		return Money{}, invalid(CodeOverflow, "amount exceeds int64 minor units")
	}
	return NewMoney(minor, currency)
}

func (m Money) Minor() int64     { return m.minor }
func (m Money) Currency() string { return m.currency }
func (m Money) Valid() bool      { return validCurrency(m.currency) }

func (m Money) check(other Money) error {
	if !m.Valid() || !other.Valid() {
		return invalid(CodeInvalidMoney, "uninitialized monetary value")
	}
	if m.currency != other.currency {
		return invalid(CodeCurrencyMismatch, "currencies must match")
	}
	return nil
}

func (m Money) Add(other Money) (Money, error) {
	if err := m.check(other); err != nil {
		return Money{}, err
	}
	if (other.minor > 0 && m.minor > math.MaxInt64-other.minor) ||
		(other.minor < 0 && m.minor < math.MinInt64-other.minor) {
		return Money{}, invalid(CodeOverflow, "monetary addition overflows")
	}
	return NewMoney(m.minor+other.minor, m.currency)
}

func (m Money) Subtract(other Money) (Money, error) {
	if err := m.check(other); err != nil {
		return Money{}, err
	}
	if (other.minor > 0 && m.minor < math.MinInt64+other.minor) ||
		(other.minor < 0 && m.minor > math.MaxInt64+other.minor) {
		return Money{}, invalid(CodeOverflow, "monetary subtraction overflows")
	}
	return NewMoney(m.minor-other.minor, m.currency)
}

func (m Money) Negate() (Money, error) {
	if !m.Valid() {
		return Money{}, invalid(CodeInvalidMoney, "uninitialized monetary value")
	}
	if m.minor == math.MinInt64 {
		return Money{}, invalid(CodeOverflow, "monetary negation overflows")
	}
	return NewMoney(-m.minor, m.currency)
}

func (m Money) Compare(other Money) (int, error) {
	if err := m.check(other); err != nil {
		return 0, err
	}
	if m.minor < other.minor {
		return -1, nil
	}
	if m.minor > other.minor {
		return 1, nil
	}
	return 0, nil
}

func (m Money) String() string {
	if !m.Valid() {
		return "<invalid money>"
	}
	// Format the signed integer first; never negate MinInt64.
	s := strconv.FormatInt(m.minor, 10)
	prefix := ""
	if s[0] == '-' {
		prefix, s = "-", s[1:]
	}
	for len(s) < 3 {
		s = "0" + s
	}
	return prefix + s[:len(s)-2] + "." + s[len(s)-2:]
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.Valid() {
		return nil, invalid(CodeInvalidMoney, "cannot serialize uninitialized money")
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{Amount: m.String(), Currency: m.currency})
}
