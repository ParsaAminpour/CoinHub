package tasks

import (
	// "coinhub/internal/adapter/tasks"
	"coinhub/internal/adapter/repository/postgres"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	FailedTransactionStatus     = 0x0
	SuccessfulTransactionStatus = 0x1
)

type PendingTransactionPayload struct {
	TrxHash string `json:"trx_hash"`
	ChainId int    `json:"chain_id"`
}

func NewPendingTransactionPayload(trxHash string, chainId int) (*asynq.Task, error) {
	payload, err := json.Marshal(PendingTransactionPayload{
		TrxHash: trxHash,
		ChainId: chainId,
	})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(EvmTransactionUpdateStatusV1, payload), nil
}

func EnqueuPendingTransactions(ctx context.Context, asynqClient *asynq.Client, trxHash string, chainId int) error {
	task, err := NewPendingTransactionPayload(trxHash, chainId)
	if err != nil {
		return err
	}
	info, err := asynqClient.EnqueueContext(ctx, task,
		asynq.Queue("transaction"),
		asynq.MaxRetry(50),
		asynq.Timeout(60*time.Second),
		asynq.Retention(24*time.Hour),   // how long to keep the task in the queue
		asynq.ProcessIn(10*time.Second), // how long to wait before processing the task
	)
	if err != nil {
		return err
	}
	zap.S().Infow("Enqueued update pending orders task", "task_id", info.ID, "queue", info.Queue, "max_retry", info.MaxRetry, "timeout", info.Timeout, "trxHash", trxHash)
	return nil
}

func HandleTransactionStatus(ctx context.Context, t *asynq.Task, db *gorm.DB, asynqClient *asynq.Client, ethClient *ethclient.Client, transactionRepository *repositories.EVMTransactionRepository) error {
	var payload PendingTransactionPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return err
	}
	zap.S().Infow("Processing transaction status", "trx_hash", payload.TrxHash, "trx_hash", payload.TrxHash)
	evmTransactionRepo := postgres.NewEVMTransactionRepository(db)

	txHash := common.HexToHash(payload.TrxHash)
	zap.S().Infow("Checking transaction status onchain", "txHash", txHash.String())

	var evmTx *entities.EvmTransaction
	if err := evmTransactionRepo.GetTransactionByHash(ctx, evmTx, txHash.String()); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			zap.S().Warnw("transaction not found in database", "trx_hash", txHash.String())
		}
		return err
	}

	tx, isPending, err := ethClient.TransactionByHash(ctx, txHash)
	if err != nil {
		zap.S().Errorw("failed to get transaction by hash", "error", err, "trx_hash", payload.TrxHash)
		return err
	}

	receipt, err := ethClient.TransactionReceipt(ctx, tx.Hash())
	if err != nil {
		return err
	}
	zap.S().Infow("The receipt of the transaction", "receipt", receipt)

	if isPending { // re-enqueue it
		if err := EnqueuPendingTransactions(ctx, asynqClient, txHash.String(), evmTx.Chain_id); err != nil {
			zap.S().Errorw("failed to re-enqueue pending transaction", "error", err, "trx_hash", txHash.String())
			return err
		}
		return fmt.Errorf("transaction is still pending")
	}

	if receipt.Status == SuccessfulTransactionStatus {
		if err := evmTransactionRepo.UpdateTransactionStatus(ctx, txHash.String(), entities.ConfirmedStatus); err != nil {
			return err
		}
		zap.S().Infow("Transaction confirmed", "txHash", txHash.String(), "status", "confirmed")
	} else if receipt.Status == FailedTransactionStatus {
		if err := evmTransactionRepo.UpdateTransactionStatus(ctx, txHash.String(), entities.FailedStatus); err != nil {
			return err
		}
		zap.S().Infow("Transaction failed", "txHash", txHash.String(), "status", "failed")
	}

	return nil
}
