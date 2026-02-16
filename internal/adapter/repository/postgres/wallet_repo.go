package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type WalletAccountRepository struct {
	db *gorm.DB
}

func NewWalletRepository(db *gorm.DB) repositories.WalletAccountRepository {
	return WalletAccountRepository{db: db}
}

func (w WalletAccountRepository) CreateNewWallet(ctx context.Context, walletService services.WalletService, userID uuid.UUID) (string, error) {
	generatedWalletAccount, err := walletService.GenerateWalletAddress(userID)
	if err != nil {
		return "", err
	}
	walletAccountEntity := entities.NewWalletAccount(
		userID,
		generatedWalletAccount.Address.String(),
		entities.SpotAccount,
		entities.ActiveWalletAccount,
		"",
		"",
		true,
	)

	if err := w.db.WithContext(ctx).Create(&walletAccountEntity).Error; err != nil {
		return "", err
	}
	return *walletAccountEntity.WalletAddress, nil
}

func (w WalletAccountRepository) UpdateWalletAccountStatus(ctx context.Context, userID uuid.UUID, newStatus entities.WalletAccountStatus, statusReason, frozenReason string) error {
	if err := w.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Updates(&entities.WalletAccount{
			Status:       newStatus,
			StatusReason: &statusReason,
			FrozenReason: &frozenReason,
		}).Error; err != nil {
		return err
	}
	return nil
}

func (w WalletAccountRepository) GetByUserID(ctx context.Context,
	userID uuid.UUID) (*entities.WalletAccount, error) {
	var walletAccount entities.WalletAccount
	if err := w.db.WithContext(ctx).Where("user_id = ?", userID).First(&walletAccount).Error; err != nil {
		return nil, err
	}
	return &walletAccount, nil
}
