package services

import (
	"fmt"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/google/uuid"
	hdwallet "github.com/miguelmota/go-ethereum-hdwallet"
)

type WalletService interface {
	GenerateWalletAddress(userID uuid.UUID) (accounts.Account, error)
}

type walletService struct {
	ledger *hdwallet.Wallet
}

func NewWalletService(ledger *hdwallet.Wallet) WalletService {
	return &walletService{
		ledger: ledger,
	}
}

func (ws *walletService) GenerateWalletAddress(userID uuid.UUID) (accounts.Account, error) {
	userNumber := userID.ID()
	return ws.generateNewPublicKey(userNumber)
}

// getBIP44Format returns the BIP-44 derivation path format
// 44' → BIP-44 standard
// The ' means hardened derivation (private-key–only derivation).
// 60 → Ethereum (assigned by SLIP-44).
// 0' → account - Wallet UIs call this "Account 1, Account 2…".
// 0  → change
// 0  → address index - This is what you increment to generate one address per user.
func (ws *walletService) getBIP44Format(userNumber uint32) string {
	return fmt.Sprintf("m/44'/60'/0'/0/%d", userNumber)
}

// generateNewPublicKey generates a new public key/address from the HD wallet
// This is a private method - only accessible through the WalletService interface
func (ws *walletService) generateNewPublicKey(userNumber uint32) (accounts.Account, error) {
	bip44 := ws.getBIP44Format(userNumber)
	path := hdwallet.MustParseDerivationPath(bip44)
	account, err := ws.ledger.Derive(path, false)
	if err != nil {
		return accounts.Account{}, err
	}
	return account, nil
}
