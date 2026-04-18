package blockchain

import (
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/domain/repositories"
	"coinhub/pkg/token"
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

var logTransferSig = []byte("Transfer(address,address,uint256)")
var logApprovalSig = []byte("Approval(address,address,uint256)")

var logTransferSigHash = crypto.Keccak256Hash(logTransferSig)
var logApprovalSigHash = crypto.Keccak256Hash(logApprovalSig)

// BUG: the From and To field would not decode correctly and we get zero 20bytes address.
// The reason is because the the contract we are testing is not verified onchain.
type LogTransfer struct {
	From  common.Address
	To    common.Address
	Value *big.Int
}

// BUG: the From and To field would not decode correctly and we get zero 20bytes address.
// The reason is because the the contract we are testing is not verified onchain.
type LogApproval struct {
	Owner   common.Address
	Spender common.Address
	Value   *big.Int
}

func subscribeToEVMTokenLogs(
	ctx context.Context,
	query ethereum.FilterQuery,
	evmTokenLogs chan types.Log,
	client *ethclient.Client,
) (ethereum.Subscription, error) {
	sub, err := client.SubscribeFilterLogs(ctx, query, evmTokenLogs)
	if err != nil {
		return nil, err
	}
	return sub, nil
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
	tokenContractABI, err := token.TokenMetaData.GetAbi()
	if err != nil {
		return err
	}

	var sub ethereum.Subscription
	sub, err = subscribeToEVMTokenLogs(ctx, query, evmTokenLogs, client)
	if err != nil {
		return err
	}

	for {
		select {
		case err := <-sub.Err():
			zap.S().Errorw("subscription error in EVM token log listener", "error", err)
			// re-subscribining to the EVM token log listener
			reconnectedSub, err := subscribeToEVMTokenLogs(ctx, query, evmTokenLogs, client)
			if err != nil {
				zap.S().Errorw("failed to reconnect to EVM token log listener", "error", err)
				continue
			}
			sub = reconnectedSub
			continue

		case tLog := <-evmTokenLogs:
			switch tLog.Topics[0].Hex() {
			case logTransferSigHash.Hex():
				var transferLog token.TokenTransfer
				if err := tokenContractABI.UnpackIntoInterface(&transferLog, "Transfer", tLog.Data); err != nil {
					zap.S().Errorw("failed to unpack Transfer event", "error", err, "log", tLog)
				}
				transferLog.From = common.HexToAddress(tLog.Topics[1].Hex())
				transferLog.To = common.HexToAddress(tLog.Topics[2].Hex())
				zap.S().Debugw(
					"TransferLog",
					"contractAddress", tLog.Address.Hex(),
					"hash", tLog.TxHash.Hex(),
					"from", transferLog.From.Hex(),
					"to", transferLog.To.Hex(),
					"value", transferLog.Value.String(),
				)
				if err := handleTransferHunt(ctx, asynqClient, pendingTransactionsCache, *walletRepo, tLog.TxHash.Hex(), transferLog.From.Hex(), transferLog.To.Hex(), transferLog.Value.String(), tLog.Address.String(), tLog.BlockNumber, tLog.Removed); err != nil {
					zap.S().Errorw("handleTransferHunt failed", "error", err)
				}

			case logApprovalSigHash.Hex():
				var approvalLog token.TokenApproval
				if err := tokenContractABI.UnpackIntoInterface(&approvalLog, "Approval", tLog.Data); err != nil {
					zap.S().Errorw("failed to unpack Approval event", "error", err, "log", tLog)
				}
				approvalLog.Owner = common.HexToAddress(tLog.Topics[1].Hex())
				approvalLog.Spender = common.HexToAddress(tLog.Topics[2].Hex())
				zap.S().Debugw(
					"ApprovalLog",
					"contractAddress", tLog.Address.Hex(),
					"hash", tLog.TxHash.Hex(),
					"owner", approvalLog.Owner.Hex(),
					"spender", approvalLog.Spender.Hex(),
					"value", approvalLog.Value.String(),
				)
			}
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
	isRemoved bool,
) error {
	// just check if the transaction is pending in the cache
	if _, err := pendingTransactionsCache.GetPendingTransaction(ctx, trxHash); err != nil {
		zap.S().Warnw("transaction is not pending", "error", err, "trxHash", trxHash)
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
		isRemoved,
	)
}

func handleApprovalHunt(ctx context.Context) error {
	return nil
}
