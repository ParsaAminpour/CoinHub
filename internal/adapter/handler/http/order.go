package http

import (
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/engine"
	"coinhub/internal/infrastructure/market"
	order_usecases "coinhub/internal/usecases/order"
	"errors"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type OrderHandler struct {
	UserRepository repositories.UserRepository
	TxManager      repositories.TxManager
	Engine         *engine.MatchEngine
	Producer       *kafka.EngineEventProducer
	PriceFeed      market.PriceFeed
}

func NewOrderHandler(
	userRepository repositories.UserRepository,
	txManager repositories.TxManager,
	matchEngine *engine.MatchEngine,
	producer *kafka.EngineEventProducer,
	priceFeed market.PriceFeed,
) HttpAPIHandler {
	return &OrderHandler{
		UserRepository: userRepository,
		TxManager:      txManager,
		Engine:         matchEngine,
		Producer:       producer,
		PriceFeed:      priceFeed,
	}
}

// PlaceLimitOrderHTTPHandler godoc
// @Summary      Place limit order
// @Description  Submits a limit order to the matching engine and publishes an order event.
// @Tags         order
// @Accept       json
// @Produce      json
// @Param        request  body      schema.PlaceOrderRequest  true  "PlaceOrderRequest"
// @Success      200      {object}  helper.SuccessResponse    "Order accepted"
// @Failure      400      {object}  helper.ErrorResponse      "Invalid request body"
// @Failure      401      {object}  helper.ErrorResponse      "Unauthorized"
// @Failure      500      {object}  helper.ErrorResponse      "Internal server error"
// @Router       /v1/order/limit [post]
func PlaceLimitOrderHTTPHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*OrderHandler)
	if !ok {
		return errors.New("invalid handler context for PlaceLimitOrderHTTPHandler")
	}
	var req schema.PlaceOrderRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	userID, exist := c.Get("userID")
	if !exist {
		zap.S().Infow("userID missing in context")
	}
	parsedUserID, err := uuid.Parse(userID.(string))
	if err != nil {
		responseHelper.UnauthorizedStandard(c, "invalid user ID in token")
		return err
	}

	var user entities.User
	if err := h.UserRepository.GetUserByID(c, &user, parsedUserID); err != nil {
		return err
	}

	orderUsecases := order_usecases.NewOrderUsecases(h.TxManager)
	priceInDecimal, _ := decimal.NewFromString(req.Price)
	qtyInDecimal, _ := decimal.NewFromString(req.Qty)
	if err := orderUsecases.SubmitOrder(c, h.Engine, h.Producer, user.ID.String(), req.Symbol, entities.OrderType(req.OrderType), entities.OrderSide(req.Side), priceInDecimal, qtyInDecimal); err != nil {
		return err
	}

	zap.S().Infow("Limit order placed",
		"userID", user.ID.String(),
		"symbol", req.Symbol,
		"orderType", req.OrderType,
		"side", req.Side,
		"price", req.Price,
		"qty", req.Qty,
	)

	responseHelper.SuccessStandard(c, helper.SuccessResponse{
		Success: true,
		Message: "Limit order accepted",
	})
	return nil
}

// PlaceMarketOrderHTTPHandler godoc
// @Summary      Place market order
// @Description  Submits a market order to the matching engine using the latest market price and publishes an order event.
// @Tags         order
// @Accept       json
// @Produce      json
// @Param        request  body      schema.PlaceOrderRequest  true  "PlaceOrderRequest"
// @Success      200      {object}  helper.SuccessResponse    "Order accepted"
// @Failure      400      {object}  helper.ErrorResponse      "Invalid request body"
// @Failure      401      {object}  helper.ErrorResponse      "Unauthorized"
// @Failure      500      {object}  helper.ErrorResponse      "Internal server error"
// @Router       /v1/order/market [post]
func PlaceMarketOrderHTTPHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*OrderHandler)
	if !ok {
		return errors.New("invalid handler context for PlaceMarketOrderHTTPHandler")
	}
	var req schema.PlaceOrderRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	userID, exist := c.Get("userID")
	if !exist {
		zap.S().Infow("userID missing in context")
	}
	parsedUserID, err := uuid.Parse(userID.(string))
	if err != nil {
		responseHelper.UnauthorizedStandard(c, "invalid user ID in token")
		return err
	}

	var user entities.User
	if err := h.UserRepository.GetUserByID(c, &user, parsedUserID); err != nil {
		return err
	}

	parts := strings.SplitN(req.Symbol, "-", 2)
	marketPrice, err := h.PriceFeed.GetPrice(parts[0], parts[1])
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, "price feed interrupted")
		return err
	}

	orderUsecases := order_usecases.NewOrderUsecases(h.TxManager)
	qtyInDecimal, _ := decimal.NewFromString(req.Qty)
	if err := orderUsecases.SubmitOrder(c, h.Engine, h.Producer, user.ID.String(), req.Symbol, entities.OrderTypeMarket, entities.OrderSide(req.Side), marketPrice, qtyInDecimal); err != nil {
		return err
	}

	zap.S().Infow("Market order placed",
		"userID", user.ID.String(),
		"symbol", req.Symbol,
		"side", req.Side,
		"qty", req.Qty,
	)

	responseHelper.SuccessStandard(c, helper.SuccessResponse{
		Success: true,
		Message: "Market order accepted",
	})
	return nil
}

// CancelOrderHTTPHandler godoc
// @Summary      Cancel an order
// @Description  Cancels an active limit order by order ID or client order ID.
// @Tags         order
// @Accept       json
// @Produce      json
// @Param        request  body      schema.CancelOrderRequest  true  "CancelOrderRequest"
// @Success      200      {object}  helper.SuccessResponse     "Order cancelled"
// @Failure      400      {object}  helper.ErrorResponse       "Invalid request body"
// @Failure      401      {object}  helper.ErrorResponse       "Unauthorized"
// @Failure      500      {object}  helper.ErrorResponse       "Internal server error"
// @Router       /v1/order/cancel [delete]
func CancelOrderHTTPHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*OrderHandler)
	if !ok {
		return errors.New("invalid handler context for CancelOrderHTTPHandler")
	}
	var req schema.CancelOrderRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		zap.S().Error("Failed to bind cancel order request: ", err)
		responseHelper.InvalidRequestBody(c)
		return err
	}

	userID, exist := c.Get("userID")
	if !exist {
		zap.S().Infow("userID missing in context")
	}
	parsedUserID, err := uuid.Parse(userID.(string))
	if err != nil {
		responseHelper.UnauthorizedStandard(c, "invalid user ID in token")
		return err
	}

	var user entities.User
	if err := h.UserRepository.GetUserByID(c, &user, parsedUserID); err != nil {
		zap.S().Error("Failed to get user by ID: ", err)
		return err
	}

	orderUsecases := order_usecases.NewOrderUsecases(h.TxManager)
	if err := orderUsecases.CancelLimitOrder(c, h.Engine, h.Producer, user.ID.String(), req.Symbol, entities.OrderTypeCancel, entities.OrderSide(req.Side), decimal.Zero, decimal.Zero); err != nil {
		zap.S().Error("Failed to cancel order: ", err)
		return err
	}

	responseHelper.SuccessStandard(c, helper.SuccessResponse{
		Success: true,
		Message: "cancel order request sent",
	})
	return nil
}
