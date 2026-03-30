package blockchain

import (
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/domain/repositories"
	"coinhub/pkg/token"
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

var logTransferSig = []byte("Transfer(address,address,uint256)")
var logApprovalSig = []byte("Approval(address,address,uint256)")

var logTransferSigHash = crypto.Keccak256(logTransferSig)
var logApprovalSigHash = crypto.Keccak256(logApprovalSig)

type LogTransfer struct {
	From  common.Address
	To    common.Address
	Value *big.Int
	Raw   types.Log
}

type LogApproval struct {
	Owner   common.Address
	Spender common.Address
	Value   *big.Int
	Raw     types.Log
}

// NOTE : the tokenContractAddresses come from the Database related to our supported assets
func startEVMTokenBalanceListener(
	ctx context.Context,
	asynqClient *asynq.Client,
	client *ethclient.Client,
	walletRepo *repositories.WalletAccountRepository,
	tokenContractAddresses []common.Address,
	pendingTransactionsCache *cache.PendingTransactionsCache,
	fromBlock int,
) error {
	evmTokenLogs = make(chan types.Log)

	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		Addresses: tokenContractAddresses,
	}
	tokenContractABI, err := abi.JSON(strings.NewReader(string(token.TokenABI)))
	if err != nil {
		return err
	}

	sub, err := client.SubscribeFilterLogs(ctx, query, evmTokenLogs)
	if err != nil {
		return err
	}

	for {
		select {
		case err := <-sub.Err():
			zap.S().Fatalw("subscription error in EVM token log listener", "error", err)
			// restart from the last lost block
			continue

		case tLog := <-evmTokenLogs:
			switch tLog.Topics[0].Hex() {
			case common.BytesToHash(logTransferSigHash).Hex():
				var transferLog LogTransfer
				if err := tokenContractABI.UnpackIntoInterface(&transferLog, "Transfer", tLog.Data); err != nil {
					zap.S().Errorw("failed to unpack Transfer event", "error", err, "log", tLog)
				}
				zap.S().Debugw(
					"TransferLog",
					"hash", tLog.TxHash.Hex(),
					"from", transferLog.From.Hex(),
					"to", transferLog.To.Hex(),
					"value", transferLog.Value.String(),
					"raw", transferLog.Raw,
				)
				if err := handleTransferHunt(ctx, asynqClient, pendingTransactionsCache, *walletRepo, tLog.TxHash.Hex(), transferLog.From.Hex(), transferLog.To.Hex(), transferLog.Value.String(), tLog.Address.String(), tLog.BlockNumber); err != nil {
					zap.S().Errorw("handleTransferHunt failed", "error", err)
				}

			case common.BytesToHash(logApprovalSigHash).Hex():
				var approvalLog LogApproval
				if err := tokenContractABI.UnpackIntoInterface(&approvalLog, "Approval", tLog.Data); err != nil {
					zap.S().Errorw("failed to unpack Approval event", "error", err, "log", tLog)
				}
				zap.S().Debugw(
					"ApprovalLog",
					"hash", tLog.TxHash.Hex(),
					"tokenOwner", approvalLog.Owner.Hex(),
					"spender", approvalLog.Spender.Hex(),
					"value", approvalLog.Value.String(),
					"raw", approvalLog.Raw,
				)
			}
			zap.S().Info("-------------------------------")
		}
	}
}

// NOTE: The scenario when both 'from' and 'to' belong to the system is an internal transfer, which will be handled by its appropriate handler.
func handleTransferHunt(
	ctx context.Context,
	asynqClient *asynq.Client,
	pendingTransactionsCache *cache.PendingTransactionsCache,
	walletRepo repositories.WalletAccountRepository,
	trxHash, from, to, value, tokenCA string,
	blockNumner uint64,
) error {
	// just check if the transaction is pending in the cache
	if _, err := pendingTransactionsCache.GetPendingTransaction(ctx, trxHash); err != nil {
		zap.S().Errorw("transaction is not pending", "error", err, "trxHash", trxHash)
		return fmt.Errorf("transaction is not pending")
	}
	zap.S().Infow("transaction is pending in the cache and will be handled by the task handler", "trxHash", trxHash)

	return tasks.EnqueueTransferEventTask(
		ctx,
		asynqClient,
		trxHash,
		from,
		to,
		value,
		tokenCA,
		blockNumner,
	)
}
