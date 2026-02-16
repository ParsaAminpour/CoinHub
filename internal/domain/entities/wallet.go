package entities

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type AccountType string
type WalletAccountStatus string

const (
	SpotAccount AccountType = "spot_account"
	PerpAccount AccountType = "perp_account"

	ActiveWalletAccount    WalletAccountStatus = "ACTIVE"
	ClosedWalletAccount    WalletAccountStatus = "CLOSED"    // Means withdraw is unabled
	SuspendedWalletAccount WalletAccountStatus = "SUSPENDED" // Means withdraw is unabled
)

// Wallet Account is an EOA wallet derrived from the central ledger private key for each user
// The balance and assets associated to the this wallet account won't be recorded in DB and will directly to inquire onchain.
type WalletAccount struct {
	gorm.Model

	UserID          uuid.UUID `gorm:"type:uuid;not null;uniqueIndex"` // Unique since User hasOne WalletAccount
	AccountType     AccountType
	WalletAddress   *string
	Status          WalletAccountStatus
	DepositsEnabled bool `gorm:"default:true;not null"`
	StatusReason    *string
	FrozenReason    *string
}

func NewWalletAccount(
	userID uuid.UUID,
	walletAddress string,
	accountType AccountType,
	status WalletAccountStatus,
	statusReason string,
	frozenStatus string,
	depositEnabled bool,
) WalletAccount {
	return WalletAccount{
		UserID:          userID,
		AccountType:     accountType,
		WalletAddress:   &walletAddress,
		Status:          status,
		StatusReason:    &statusReason,
		FrozenReason:    &frozenStatus,
		DepositsEnabled: depositEnabled,
	}
}

func (WalletAccount) TableName() string {
	return "wallet_account"
}

// cached balances for performance (careful)
// available_balance (decimal, high precision)
// locked_balance (decimal) — reserved for open orders/withdrawals
// pending_balance (decimal) — awaiting confirmations/settlement
// balance_version (bigint) — optimistic concurrency
// last_ledger_entry_id (nullable) — where cache was computed up to
// last_reconciled_at (timestamp)
