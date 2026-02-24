package user_usecases

import (
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
)

type VerifyGmailUsecases struct {
	userRepository repositories.UserRepository
}

// repo is injected from entities:repositories
func NewVerifyGmailUsecases(userRepo repositories.UserRepository) VerifyGmailUsecases {
	return VerifyGmailUsecases{userRepository: userRepo}
}

func (vrf VerifyGmailUsecases) LabelGmailVerificationStatus(ctx context.Context, gmail string, newVerificationStatus entities.GmailVerificationStatus) error {
	if err := vrf.userRepository.UpdateGmailVerificationStatus(ctx, gmail, newVerificationStatus); err != nil {

	}
	return nil
}
