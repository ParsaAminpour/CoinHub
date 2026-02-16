package services

import (
	"coinhub/pkg/token"
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	hdwallet "github.com/miguelmota/go-ethereum-hdwallet"
)

type WalletService interface {
	GenerateWalletAddress(userID uuid.UUID) (accounts.Account, error)
}

type walletService struct {
	ledger *hdwallet.Wallet
	client *ethclient.Client
}

func NewWalletService(ledger *hdwallet.Wallet, client *ethclient.Client) WalletService {
	return &walletService{
		ledger: ledger,
		client: client,
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

// construct transaction
// sign transaction
// send transaction
// wait for confirmation
// NOTE : The gas limit for a standard ETH transfer is 21000 units.
// NOTE : Gas price that will get your transaction included pretty fast in a block is 30 gwei, 0 means use the network suggestion.
func (
	ws *walletService,
) createNativeTransferTx(
	ctx context.Context,
	client *ethclient.Client,
	fromPk string,
	toPk string,
	amountWei *big.Int,
	gasLimitUnit uint64,
	gasPriceWei *big.Int,
) (*types.Transaction, error) {
	fromAddress := common.HexToAddress(fromPk)
	toAddress := common.HexToAddress(toPk)
	nonce, err := client.PendingNonceAt(ctx, fromAddress)
	if err != nil {
		return nil, err
	}
	if gasPriceWei == big.NewInt(0) {
		gasPriceWei, err = client.SuggestGasPrice(context.Background())
		if err != nil {
			return nil, err
		}
	}
	tx := types.NewTransaction(nonce, toAddress, amountWei, gasLimitUnit, gasPriceWei, nil)
	return tx, nil
}

func (ws *walletService) ethSignTransaction(ctx context.Context, tx *types.Transaction, account accounts.Account) (*types.Transaction, error) {
	chainId, err := ws.client.NetworkID(ctx)
	if err != nil {
		return nil, err
	}
	rawPrivateKey, err := ws.ledger.PrivateKey(account)
	if err != nil {
		return nil, err
	}
	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainId), rawPrivateKey)
	if err != nil {
		return nil, err
	}
	return signedTx, nil
}

func (ws *walletService) sendSignedTransaction(ctx context.Context, signedTx *types.Transaction) (string, error) {
	if err := ws.client.SendTransaction(ctx, signedTx); err != nil {
		return "", err
	}
	return signedTx.Hash().Hex(), nil
}

// GetNativeETHBalance retrieves the native ETH balance (in wei) for the given wallet address.
func GetNativeETHBalance(ctx context.Context, ethClient *ethclient.Client, walletAddress string) (*big.Int, error) {
	account := common.HexToAddress(walletAddress)
	balance, err := ethClient.BalanceAt(ctx, account, nil)
	if err != nil {
		return big.NewInt(0), err
	}
	return balance, nil
}

// GetTokenBalance retrieves the balance of a specific ERC-20 token for the given wallet address.
func GetTokenBalance(ctx context.Context, ethClient *ethclient.Client, tokenContractAddress, walletAddress string) (*big.Int, error) {
	tokenAddress := common.HexToAddress(tokenContractAddress)
	callerAddress := common.HexToAddress(walletAddress)
	instance, err := token.NewToken(tokenAddress, ethClient)
	if err != nil {
		return big.NewInt(0), err
	}
	balance, err := instance.BalanceOf(&bind.CallOpts{}, callerAddress)
	if err != nil {
		return big.NewInt(0), err
	}
	return balance, nil
}
