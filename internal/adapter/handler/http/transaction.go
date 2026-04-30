package http

import (
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"coinhub/internal/usecases/wallet_usecases"
	"errors"
	"fmt"
	"math/big"
	"net/http"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type TransactionHandler struct {
	UserRepository          repositories.UserRepository
	WalletAccountRepository repositories.WalletAccountRepository
	TransactionRepository   repositories.EVMTransactionRepository
	WalletService           services.WalletService
	ETHClient               *ethclient.Client
	AsynqClient             *asynq.Client
	PendingTransactionsCache *cache.PendingTransactionsCache
}

func NewTransactionHandler(
	userRepository repositories.UserRepository,
	walletAccountRepository repositories.WalletAccountRepository,
	transactionRepository repositories.EVMTransactionRepository,
	walletService services.WalletService,
	ethClient *ethclient.Client,
	asynqClient *asynq.Client,
	pendingTransactionsCache *cache.PendingTransactionsCache,
) HttpAPIHandler {
	return &TransactionHandler{
		UserRepository:           userRepository,
		WalletAccountRepository:  walletAccountRepository,
		TransactionRepository:    transactionRepository,
		WalletService:            walletService,
		ETHClient:                ethClient,
		AsynqClient:              asynqClient,
		PendingTransactionsCache: pendingTransactionsCache,
	}
}

// WithdrawHandler godoc
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
func WithdrawHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*TransactionHandler)
	if !ok {
		return errors.New("invalid handler context for WithdrawHandler")
	}
	var req schema.WithdrawNativeRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	userID, exist := c.Get("userID")
	if !exist {
		zap.S().Infow("userID missing in context")
	}
	zap.S().Infow("userID retrieved from jwt", "userID", userID.(string))

	parsedUserID, err := uuid.Parse(userID.(string))
	if err != nil {
		responseHelper.UnauthorizedStandard(c, "invalid user ID in token")
		return err
	}

	var user entities.User
	if err := h.UserRepository.GetUserByID(c, &user, parsedUserID); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			responseHelper.NotFoundStandard(c, "user not found")
			return err
		}
		responseHelper.InternalServerErrorStandard(c, helper.MsgInternalError)
		return err
	}

	if req.AssetOwnerAddress != user.WalletAccount.WalletAddress {
		responseHelper.UnauthorizedStandard(c, "the caller is not the asset owner or origin address")
		return fmt.Errorf("the caller is not the asset owner or origin address")
	}

	walletAccount, err := h.WalletAccountRepository.GetByUserID(c, user.ID)
	if err != nil {
		responseHelper.NotFoundStandard(c, "user's wallet not found")
		return err
	}
	account, err := h.WalletService.GetWalletAccountByUserID(uint32(walletAccount.WalletAddressIndex))
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, helper.MsgInternalError)
		return err
	}
	zap.S().Infow("the account has retrieved", "account", account.Address.Hex())

	walletUsecases := wallet_usecases.NewWalletUsecases(
		h.UserRepository,
		h.WalletAccountRepository,
		h.TransactionRepository,
		h.WalletService,
	)

	amountInWeiBigint, _ := new(big.Int).SetString(req.AmountWei, 10)
	gasPriceWeiBigInt, _ := new(big.Int).SetString(req.GasPriceWei, 10)

	txHash, err := walletUsecases.WithdrawAsset(c, user.ID, h.AsynqClient, h.ETHClient, h.PendingTransactionsCache, &account, req.DestinationAddress, amountInWeiBigint, uint64(req.GasLimitUnit), gasPriceWeiBigInt, uint32(req.ChainId), req.Calldata)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	zap.S().Infow("transaction submitted", "txHash", txHash)

	responseHelper.SuccessStandard(c, schema.WithdrawNativeResponse{
		Code:              http.StatusOK,
		TransactionStatus: string(entities.PendingStatus),
	})
	return nil
}
