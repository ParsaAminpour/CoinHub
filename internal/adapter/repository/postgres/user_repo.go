package postgres

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type UserRepository struct {
	db *gorm.DB
}

func NewUserRepository(db *gorm.DB) repositories.UserRepository {
	return &UserRepository{db: db}
}

func (r *UserRepository) Create(ctx context.Context, user *entities.User) error {
	if err := r.db.WithContext(ctx).Create(&user).Error; err != nil {
		return err
	}
	return nil
}

func (r *UserRepository) Delete(ctx context.Context, userId uuid.UUID) error {
	if err := r.db.Where("user_id = ?", userId).Delete(ctx).Error; err != nil {
		return err
	}
	return nil
}

func (r *UserRepository) UpdateGmailVerificationStatus(ctx context.Context, gmail string, gmailVerificationStatus entities.GmailVerificationStatus) error {
	var isVerified bool
	if gmailVerificationStatus == entities.GmailVerificationStatusVerified {
		isVerified = true
	} else {
		isVerified = false
	}
	if err := r.db.WithContext(ctx).
		Model(&entities.User{}).
		Where("gmail = ? ", gmail).
		Update("gmail_verification_status", gmailVerificationStatus).
		Update("is_verified", isVerified).Error; err != nil {
		return err
	}
	return nil
}

func (r *UserRepository) GetUserByID(ctx context.Context, user *entities.User, userID uuid.UUID) error {
	if err := r.db.WithContext(ctx).Where("id = ?", userID).Preload("WalletAccount").Preload("Role").First(&user).Error; err != nil {
		return err
	}
	return nil
}

func (r *UserRepository) GetUserByUsername(ctx context.Context, user *entities.User, username string) error {
	if err := r.db.WithContext(ctx).
		Where("username = ?", username).
		Preload("WalletAccount").
		First(&user).Error; err != nil {
		return err
	}
	return nil
}

var user entities.User

func (r *UserRepository) GetUserByGmail(ctx context.Context, user *entities.User, gmail string) error {
	if err := r.db.WithContext(ctx).
		Where("gmail = ?", gmail).
		Preload("WalletAccount").
		First(&user).Error; err != nil {
		return err
	}
	return nil
}

func (r *UserRepository) GetUserByWalletAccount(ctx context.Context, user *entities.User, walletAddress string) error {
	if err := r.db.
		Joins("JOIN wallet_account ON wallet_account.user_id = users.id").
		Where("wallet_account.wallet_address = ?", walletAddress).
		Preload("WalletAccount").
		First(user).Error; err != nil {
		return err
	}
	return nil
}
