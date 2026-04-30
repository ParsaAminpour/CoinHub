package tasks

import (
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

type TransferEventPayload struct {
	TrxHash     string
	BlockNumber uint64
	From        string
	To          string
	IsRemoved   bool
	// IsReceiver  bool // the IsReceiver will be determined by the task handler
	value   string
	TokenCA string
	Time    time.Time
}

func RegisterTransferEventTask(trxHash, from, to, value, tokenCA string, blockNumner uint64, isRemoved bool) (*asynq.Task, error) {
	transferEventPayload, err := json.Marshal(TransferEventPayload{
		TrxHash:     trxHash,
		BlockNumber: blockNumner,
		From:        from,
		To:          to,
		IsRemoved:   isRemoved,
		value:       value,
		TokenCA:     tokenCA,
		Time:        time.Unix(time.Now().Unix(), 0),
	})
	if err != nil {
		return nil, err
	}
	task := asynq.NewTask(TransferEventCreateV1, transferEventPayload, nil)
	return task, nil
}

func EnqueueTransferEventTask(ctx context.Context, asynqClient *asynq.Client, trxHash, from, to, value, tokenCA string, blockNumner uint64, isRemoved bool) error {
	task, err := RegisterTransferEventTask(trxHash, from, to, value, tokenCA, blockNumner, isRemoved)
	if err != nil {
		return err
	}
	info, err := asynqClient.EnqueueContext(ctx, task,
		asynq.Queue("transaction"),
		asynq.MaxRetry(2),
		asynq.Timeout(60*time.Second),
		asynq.Retention(1*time.Hour),   // how long to keep the task in the queue
		asynq.ProcessIn(5*time.Second), // how long to wait before processing the task
	)
	if err != nil {
		return err
	}
	zap.S().Infow("Enqueued update transfer task", "task_id", info.ID, "queue", info.Queue, "max_retry", info.MaxRetry, "timeout", info.Timeout, "trxHash", trxHash)
	return nil
}

// NOTE: Here we caught up a transaction that is related to our user, now we are going to check the transaction and update its relevant status.
func HandleTransferEventTask(ctx context.Context, t *asynq.Task, ethClient *ethclient.Client, walletRepo repositories.WalletAccountRepository, transferEventRepo repositories.TransferEventRepository, assetRepo repositories.AssetRepository, pendingTransactionsCache *cache.PendingTransactionsCache) error {
	var payload TransferEventPayload
	err := json.Unmarshal(t.Payload(), &payload)
	if err != nil {
		zap.S().Errorw("Failed to unmarshal transfer event payload", "error", err, "task_payload", string(t.Payload()))
		return err
	}

	zap.S().Infow("Handling transfer event task",
		"trxHash", payload.TrxHash,
		"blockNumber", payload.BlockNumber,
		"from", payload.From,
		"to", payload.To,
		"value", payload.value,
		"tokenCA", payload.TokenCA,
		"time", payload.Time,
	)

	var isReceiver bool
	if _, err := walletRepo.GetByWalletAddress(ctx, payload.To); err != nil {
		isReceiver = true
	} else if _, err := walletRepo.GetByWalletAddress(ctx, payload.From); err != nil {
		isReceiver = false
	} else {
		return fmt.Errorf("both from and to address are not belong to the system")
	}

	var infectedAddress string
	if infectedAddress = payload.To; !isReceiver {
		infectedAddress = payload.From
	}
	// Tag the wallet account balance as synced
	if err := walletRepo.UpdateTheBalanceSync(ctx, infectedAddress, isReceiver, payload.Time); err != nil {
		return err
	}
	zap.S().Infow("Updated wallet balance",
		"infected_address", infectedAddress,
		"is_receiver", isReceiver,
	)

	asset, err := assetRepo.GetAssetByCotnractAddress(ctx, payload.TokenCA)
	if err != nil {
		zap.S().Warnw("Failed to get asset by contract address", "error", err, "tokenCA", payload.TokenCA)
		return fmt.Errorf("asset not found")
	}

	receipt, err := ethClient.TransactionReceipt(ctx, common.HexToHash(payload.TrxHash))
	if err != nil {
		zap.S().Warnw("Failed to fetch transaction receipt, will retry", "error", err, "trxHash", payload.TrxHash)
		return fmt.Errorf("transaction receipt not available yet: %w", err)
	}

	var transferStatus entities.TransactionStatus
	switch {
	case payload.IsRemoved:
		// The log was removed due to a chain reorganization.
		transferStatus = entities.RevertedStatus
	case receipt.Status == types.ReceiptStatusFailed:
		// The transaction was included in a block but the EVM execution reverted.
		transferStatus = entities.RevertedStatus
	default:
		transferStatus = entities.ConfirmedStatus
	}
	transferEvent := entities.NewTransferEvent(
		payload.TrxHash,
		payload.BlockNumber,
		asset.ID,
		payload.From,
		payload.To,
		payload.value,
		*asset.Symbol,
		transferStatus,
		payload.Time,
	)
	if err := transferEventRepo.Create(ctx, transferEvent); err != nil {
		zap.S().Errorw("Failed to create transfer event", "error", err, "transfer_event", transferEvent)
		return err
	}
	zap.S().Info("Processed transfer event successfully")

	if err := pendingTransactionsCache.DeletePendingTransaction(ctx, payload.TrxHash); err != nil {
		zap.S().Errorw("Failed to delete pending transaction from cache", "error", err, "trxHash", payload.TrxHash)
		return err
	}
	zap.S().Infow("Deleted pending transaction from cache", "trxHash", payload.TrxHash)
	return nil
}
