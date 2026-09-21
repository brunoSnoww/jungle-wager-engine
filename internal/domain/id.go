package domain

import "encoding/hex"

// IDs are supplied by infrastructure. Domain never creates random identities.
type ID [16]byte
type WalletID [16]byte
type WagerTransactionID [16]byte
type LedgerEntryID [16]byte
type EventID [16]byte
type PlayerID [16]byte

func ParseID(value string) (ID, error) {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return ID{}, invalid(CodeInvalidID, "expected canonical UUID")
	}
	compact := value[:8] + value[9:13] + value[14:18] + value[19:23] + value[24:]
	var id ID
	if _, err := hex.Decode(id[:], []byte(compact)); err != nil || id == (ID{}) {
		return ID{}, invalid(CodeInvalidID, "UUID must be nonzero hexadecimal")
	}
	return id, nil
}

func (id ID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], id[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], id[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], id[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], id[8:10])
	b[23] = '-'
	hex.Encode(b[24:36], id[10:16])
	return string(b[:])
}

func (id ID) MarshalText() ([]byte, error)                 { return []byte(id.String()), nil }
func (id WalletID) String() string                         { return ID(id).String() }
func (id WagerTransactionID) String() string               { return ID(id).String() }
func (id LedgerEntryID) String() string                    { return ID(id).String() }
func (id EventID) String() string                          { return ID(id).String() }
func (id PlayerID) String() string                         { return ID(id).String() }
func (id WalletID) MarshalText() ([]byte, error)           { return ID(id).MarshalText() }
func (id WagerTransactionID) MarshalText() ([]byte, error) { return ID(id).MarshalText() }
func (id LedgerEntryID) MarshalText() ([]byte, error)      { return ID(id).MarshalText() }
func (id EventID) MarshalText() ([]byte, error)            { return ID(id).MarshalText() }
func (id PlayerID) MarshalText() ([]byte, error)           { return ID(id).MarshalText() }
