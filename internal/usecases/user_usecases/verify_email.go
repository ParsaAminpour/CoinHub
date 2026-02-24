package user_usecases

import (
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

type VerifyGmailUsecases struct {
	userRepository repositories.UserRepository
}

// repo is injected from entities:repositories
func NewVerifyGmailUsecases(userRepo repositories.UserRepository) VerifyGmailUsecases {
	return VerifyGmailUsecases{userRepository: userRepo}
}

func (vrf VerifyGmailUsecases) LabelGmailVerificationStatus(
	ctx context.Context,
	redisClient *redis.Client,
	authGmailCache *cache.AuthGmailCache,
	userRepo repositories.UserRepository,
	gmail string,
	username string,
	verificationCode string,
	newVerificationStatus entities.GmailVerificationStatus,
) error {
	ttl, err := authGmailCache.GetGmailVerificationCodeTimeLeft(ctx, redisClient, gmail, username)
	if err != nil {
		zap.S().Errorw("failed to get gmail verification code TTL", "error", err, "gmail", gmail, "username", username)
		return fmt.Errorf("failed to check verification code TTL")
	}
	if ttl.Seconds() < 0 {
		return fmt.Errorf("Verification code has expired. Please resend the code to get new verification code.")
	}

	cachedCode, err := authGmailCache.GetGmailVerificationCode(ctx, redisClient, gmail, username)
	if err != nil {
		return fmt.Errorf("invalid or expired verification code")
	}

	if cachedCode != verificationCode {
		return fmt.Errorf("invalid verification code")
	}

	if err := userRepo.UpdateGmailVerificationStatus(ctx, gmail, entities.GmailVerificationStatusVerified); err != nil {
		return fmt.Errorf("failed to update gmail verification status")
	}
	return nil
}
