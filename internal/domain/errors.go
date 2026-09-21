// Package domain contains the financial model. It depends only on the Go standard library.
package domain

// Error is a classifiable domain failure. Codes are stable across transports.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func invalid(code, message string) error { return &Error{Code: code, Message: message} }

const (
	CodeInvalidMoney                 = "INVALID_MONEY"
	CodeInvalidCurrency              = "INVALID_CURRENCY"
	CodeInvalidAmount                = "INVALID_AMOUNT"
	CodeInvalidScale                 = "INVALID_SCALE"
	CodeOverflow                     = "OVERFLOW"
	CodeInvalidID                    = "INVALID_ID"
	CodeInvalidWallet                = "INVALID_WALLET"
	CodeInvalidWager                 = "INVALID_WAGER"
	CodeInvalidState                 = "INVALID_STATE"
	CodeInvalidLedger                = "INVALID_LEDGER"
	CodeInvalidEvent                 = "INVALID_EVENT"
	CodeInsufficientFunds            = "INSUFFICIENT_FUNDS"
	CodeInsufficientFundsForReversal = "INSUFFICIENT_FUNDS_FOR_REVERSAL"
	CodeCurrencyMismatch             = "CURRENCY_MISMATCH"
	CodeReferenceNotFound            = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed        = "REFERENCE_NOT_PROCESSED"
	CodeReferenceMismatch            = "REFERENCE_MISMATCH"
	CodeReferenceAlreadyReversed     = "REFERENCE_ALREADY_REVERSED"
	CodeInvalidAmountForKind         = "INVALID_AMOUNT_FOR_KIND"
	CodeIdempotencyKeyReused         = "IDEMPOTENCY_KEY_REUSED"
	CodeExternalTransactionIDReused  = "EXTERNAL_TRANSACTION_ID_REUSED"
)
