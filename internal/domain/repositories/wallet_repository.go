package repositories

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/services"
	"context"

	"github.com/google/uuid"
)

type WalletAccountRepository interface {
	CreateNewWallet(ctx context.Context, walletService services.WalletService, userID uuid.UUID) (string, error)
	UpdateWalletAccountStatus(ctx context.Context, userID uuid.UUID, newStatus entities.WalletAccountStatus, statusReason, frozenReason string) error
	GetByUserID(ctx context.Context, userID uuid.UUID) (*entities.WalletAccount, error)
}
