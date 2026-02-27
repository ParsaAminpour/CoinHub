package repositories

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/services"
	"context"
	"time"

	"github.com/google/uuid"
)

type WalletAccountRepository interface {
	CreateNewWallet(ctx context.Context, walletService services.WalletService, userID uuid.UUID) error
	UpdateWalletAccountStatus(ctx context.Context, userID uuid.UUID, newStatus entities.WalletAccountStatus, statusReason, frozenReason string) error
	GetByUserID(ctx context.Context, userID uuid.UUID) (*entities.WalletAccount, error)
	GetByWalletAddress(ctx context.Context, walletAddress string) (*entities.WalletAccount, error)
	UpdateTheBalanceSync(ctx context.Context, walletAddress string, flag bool, lastBalanceSyncedAtTimestamp time.Time) error
}
