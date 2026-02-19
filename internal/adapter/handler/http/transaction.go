package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/domain/entities"
	"coinhub/internal/usecases/wallet_usecases"
	"errors"
	"fmt"
	"math/big"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Withdraw handles asset withdrawal requests
// @Summary      Withdraw asset
// @Description  Handles asset withdrawal requests for native tokens.
// @Tags         transaction
// @Accept       json
// @Produce      json
// @Param        request  body      schema.WithdrawNativeRequest  true  "WithdrawNativeRequest"
// @Success      200      {object}  schema.WithdrawNativeResponse "Successful withdrawal"
// @Failure      400      {object}  helper.ErrorResponse          "Invalid request body"
// @Failure      404      {object}  helper.ErrorResponse          "User or wallet not found"
// @Failure      500      {object}  helper.ErrorResponse          "Internal server error"
// @Router       /v1/transaction/withdraw [post]
func WithdrawHandler(c *gin.Context, app *internal.Application) error {
	// TODO : Add more intense request checker
	var req schema.WithdrawNativeRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	username, exist := c.Get("username")
	if !exist {
		zap.S().Infow("username missing in context")
	}
	zap.S().Infow("username retrieved from jwt", "username", username.(string))

	app.UserRepository.GetUserByUsername(c, &user, username.(string))
	retrievedUserWalletAddress := user.WalletAccount.WalletAddress
	zap.S().Info("retrieved user wallet address", "walletAddress", user.WalletAccount.WalletAddress)
	if req.AssetOwnerAddress != retrievedUserWalletAddress {
		responseHelper.UnauthorizedStandard(c, "the caller is not the asset owner or origin address")
		return fmt.Errorf("the caller is not the asset owner or origin address")
	}

	if err := app.UserRepository.GetUserByWalletAccount(c, &user, req.AssetOwnerAddress); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			responseHelper.NotFoundStandard(c, "user not found")
			return err
		}
		responseHelper.InternalServerErrorStandard(c, helper.MsgInternalError)
		return err
	}

	walletAccount, err := app.WalletAccountRepository.GetByUserID(c, user.ID)
	if err != nil {
		responseHelper.NotFoundStandard(c, "user's wallet not found")
		return err
	}
	account, err := app.WalletService.GetWalletAccountByUserID(uint32(walletAccount.WalletAddressIndex))
	zap.S().Infow("the account has retrieved", "account", account.Address.Hex())

	// // send withdraw transaction
	walletUsecases := wallet_usecases.NewWalletUsecases(
		app.UserRepository,
		app.WalletAccountRepository,
		app.TransactinRepository,
		app.WalletService,
	)

	amountInWeiBigint, _ := new(big.Int).SetString(req.AmountWei, 10)
	gasPriceWeiBigInt, _ := new(big.Int).SetString(req.GasPriceWei, 10)

	// store a pending transactino to the database
	txHash, err := walletUsecases.WithdrawAsset(c, user.ID, app.ETHClient, &account, req.DestinationAddress, amountInWeiBigint, uint64(req.GasLimitUnit), gasPriceWeiBigInt, uint32(req.ChainId), req.Calldata)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	zap.S().Infow("transaction submitted", "txHash", txHash)

	// send the pending transaction to the asynq
	// wait until confirmations passed and then update the transaction status to fulfilled

	responseHelper.SuccessStandard(c, schema.WithdrawNativeResponse{
		Code:              http.StatusOK,
		TransactionStatus: string(entities.PendingStatus),
	})
	return nil
}
