package wallet_usecases

import (
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

type WalletUsecases struct {
	userRepo        repositories.UserRepository
	walletRepo      repositories.WalletAccountRepository
	transactionRepo repositories.EVMTransactionRepository
	walletService   services.WalletService
}

func NewWalletUsecases(
	userRepo repositories.UserRepository,
	walletRepo repositories.WalletAccountRepository,
	transactionRepo repositories.EVMTransactionRepository,
	walletService services.WalletService,
) WalletUsecases {
	return WalletUsecases{
		userRepo:        userRepo,
		walletRepo:      walletRepo,
		transactionRepo: transactionRepo,
		walletService:   walletService,
	}
}

func (
	wu *WalletUsecases,
) WithdrawAsset(
	ctx context.Context,
	callerUserID uuid.UUID,
	asynqClient *asynq.Client,
	client *ethclient.Client,
	fromAccount *accounts.Account,
	toPk string,
	amountInWei *big.Int,
	gasLimitUnit uint64,
	gasPriceWei *big.Int,
	chainId uint32, // like 11155111 for sepolia testnet
	calldata string,
) (string, error) {
	zap.S().Infow("WithdrawAsset input",
		"from", fromAccount.Address.Hex(),
		"to", toPk,
		"amountInWei", amountInWei.String(),
		"gasLimitUnit", gasLimitUnit,
		"gasPriceWei", gasPriceWei.String(),
	)
	rawTx, err := wu.walletService.CreateNativeTransferTx(ctx, client, fromAccount.Address.Hex(), toPk, amountInWei, gasLimitUnit, gasPriceWei)
	if err != nil {
		return "", err
	}
	fromAddress := common.HexToAddress(fromAccount.Address.Hex())
	fromAccountNonce, err := client.PendingNonceAt(ctx, fromAddress)
	zap.S().Infow("raw transaction has initialized", "rawTx", rawTx)

	signedTx, err := wu.walletService.EthSignTransaction(ctx, rawTx, *fromAccount)
	if err != nil {
		return "", err
	}
	zap.S().Infow("signed transaction has initialized", "signedTx", signedTx)

	trxHash, err := wu.walletService.SendSignedTransaction(ctx, signedTx)
	if err != nil {
		zap.S().Errorw("failed to send signed transaction", "error", err, "from", fromAccount.Address.Hex(), "to", toPk, "amountInWei", amountInWei.String())
		transaction := entities.NewEVMTransaction(
			callerUserID,
			nil,
			int(chainId),
			int(fromAccountNonce),
			fromAccount.Address.Hex(),
			&toPk,
			int(gasPriceWei.Int64()),
			&calldata,
			entities.EIP1559,
			entities.FailedStatus,
			err.Error(),
		)
		if err := wu.transactionRepo.CreateTransaction(ctx, &transaction); err != nil {
			zap.S().Errorw("failed to record failed transaction in DB", "error", err, "trxHash", trxHash, "from", fromAccount.Address.Hex(), "to", toPk, "amountInWei", amountInWei.String())
			return "", err
		}
		return "", err
	}
	zap.S().Infow("signed transaction sended onchain", "txHash", trxHash)

	// TODO : Add this pending transaction to the asynq queue to check the network confirmations.
	transaction := entities.NewEVMTransaction(
		callerUserID,
		&trxHash,
		int(chainId),
		int(fromAccountNonce), // TODO : set proper value here
		fromAccount.Address.Hex(),
		&toPk,
		int(gasPriceWei.Int64()),
		&calldata,
		entities.EIP1559,
		entities.PendingStatus,
		"",
	)
	if err := wu.transactionRepo.CreateTransaction(ctx, &transaction); err != nil {
		return "", err
	}
	if err := tasks.EnqueuPendingTransactions(ctx, asynqClient, trxHash, int(chainId)); err != nil {
		return "", err
	}

	zap.S().Infow("pending transaction recorded in DB", "txHash", trxHash)
	return trxHash, nil
}
