package application

import (
	"jungle/internal/domain"
	"strings"
)

// NormalizeCommand is shared by HTTP, SQS and hashing. Text identities are not
// silently trimmed; only UUID rendering and enum case have canonical forms.
func NormalizeCommand(c Command) (Command, error) {
	player, err := domain.ParseID(c.PlayerID)
	if err != nil {
		return c, err
	}
	wallet, err := domain.ParseID(c.WalletID)
	if err != nil {
		return c, err
	}
	c.PlayerID = player.String()
	c.WalletID = wallet.String()
	c.Kind = strings.ToUpper(c.Kind)
	for _, value := range []string{c.ProviderID, c.ExternalTransactionID, c.IdempotencyKey, c.RoundID, c.GameID} {
		if len(value) == 0 || len(value) > 256 || strings.TrimSpace(value) != value {
			return c, &Error{Code: "INVALID_INPUT"}
		}
	}
	if len(c.ReferenceExternalTransactionID) > 256 {
		return c, &Error{Code: "INVALID_INPUT"}
	}
	if _, err = domain.NewMoney(c.Money.Minor(), c.Money.Currency()); err != nil {
		return c, err
	}
	if c.Money.Minor() < 0 {
		return c, &Error{Code: "INVALID_AMOUNT"}
	}
	switch c.Kind {
	case "BET", "WIN", "LOSS", "REFUND", "ROLLBACK":
	default:
		return c, &Error{Code: "INVALID_KIND"}
	}
	if (c.Kind == "REFUND" || c.Kind == "ROLLBACK") && c.ReferenceExternalTransactionID == "" {
		return c, &Error{Code: "REFERENCE_REQUIRED"}
	}
	if c.CorrelationID == "" {
		c.CorrelationID = c.ExternalTransactionID
	}
	return c, nil
}
