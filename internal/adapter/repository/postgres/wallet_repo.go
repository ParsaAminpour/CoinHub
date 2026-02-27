package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type WalletAccountRepository struct {
	db *gorm.DB
}

func NewWalletRepository(db *gorm.DB) repositories.WalletAccountRepository {
	return WalletAccountRepository{db: db}
}

// NextAddressIndex allocates a unique index from Postgres sequence.
func (w WalletAccountRepository) NextAddressIndex() (uint64, error) {
	var idx uint64
	if err := w.db.Raw(`SELECT nextval('wallet_address_index_seq')`).Scan(&idx).Error; err != nil {
		zap.S().Errorw("::Failed to get next wallet address index from Postgres sequence", "error", err)
		return 0, err
	}
	return idx, nil
}

func (w WalletAccountRepository) CreateNewWallet(ctx context.Context, walletService services.WalletService, userID uuid.UUID) error {
	walletAddressIdx, err := w.NextAddressIndex()
	if err != nil {
		return err
	}
	zap.S().Infow("Allocated wallet address index", "index", walletAddressIdx)

	generatedWalletAccount, err := walletService.GenerateWalletAddress(uint32(walletAddressIdx))
	if err != nil {
		return err
	}

	walletAccountEntity := entities.NewWalletAccount(
		userID,
		generatedWalletAccount.Address.String(),
		walletAddressIdx,
		entities.SpotAccount,
		entities.ActiveWalletAccount,
		"",
		"",
		true,
	)

	if err := w.db.WithContext(ctx).Create(&walletAccountEntity).Error; err != nil {
		return err
	}
	return nil
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

func (w WalletAccountRepository) UpdateTheBalanceSync(ctx context.Context, walletAddress string, flag bool, lastBalanceSyncedAtTimestamp time.Time) error {
	updates := map[string]interface{}{
		"balance_synced":                   flag,
		"last_balance_synced_at_timestamp": lastBalanceSyncedAtTimestamp,
	}
	if err := w.db.WithContext(ctx).
		Model(&entities.WalletAccount{}).
		Where("wallet_address = ?", walletAddress).
		Updates(updates).Error; err != nil {
		return err
	}
	return nil
}

func (w WalletAccountRepository) GetByWalletAddress(ctx context.Context, walletAddress string) (*entities.WalletAccount, error) {
	var walletAccount entities.WalletAccount
	if err := w.db.WithContext(ctx).Where("wallet_address = ?", walletAddress).First(&walletAccount).Error; err != nil {
		return nil, err
	}
	return &walletAccount, nil
}
