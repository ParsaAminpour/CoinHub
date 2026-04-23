package blockchain

import (
	"coinhub/internal"
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/zap"
)

// TODO : is there any better option to store these logs instead of a package level state variable?
var (
	evmTokenLogs chan types.Log
)

func StartOnchainListener(ctx context.Context, fromBlock int, app *internal.Application) error {
	availableAssets, err := app.AssetRepository.GetAvailableAssets(ctx)
	if err != nil {
		return err
	}
	availableAssetContractAddresses := make([]common.Address, len(availableAssets))
	for i, cas := range availableAssets {
		availableAssetContractAddresses[i] = common.HexToAddress(*cas.AssetAddress)
	}

	if err := startEVMTokenBalanceListener(
		ctx,
		app.AsynqClient,
		app.ETHWebsocketClient,
		&app.WalletAccountRepository,
		availableAssetContractAddresses,
		app.PendingTransactionsCache,
		fromBlock); err != nil {
		zap.S().Errorw("failed to start EVM token balance listener", "error", err)
		return fmt.Errorf("failed to start EVM token balance listener", "error", err)
	}
	return nil
}
