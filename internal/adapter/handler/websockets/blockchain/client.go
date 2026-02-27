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

func StartOnchainListener(ctx context.Context, app *internal.Application) error {
	cas := common.HexToAddress("0x036CbD53842c5426634e7929541eC2318f3dCF7e")
	if err := startEVMTokenBalanceListener(ctx, app.AsynqClient, app.ETHWebsocketClient, &app.WalletAccountRepository, []common.Address{cas}, 10334076); err != nil {
		zap.S().Errorw("failed to start EVM token balance listener", "error", err)
		return fmt.Errorf("failed to start EVM token balance listener", "error", err)
	}

	return nil
}
