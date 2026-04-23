package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/domain/entities"
	order_usercases "coinhub/internal/usecases/order"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

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
func PlaceLimitOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	var req schema.PlaceOrderRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	userID, exist := c.Get("userID")
	if !exist {
		zap.S().Infow("userID missing in context")
	}
	parsedUserID, err := uuid.Parse(userID.(string))
	if err != nil {
		responseHelper.UnauthorizedStandard(c, "invalid user ID in token")
		return err
	}

	orderUsecases := order_usercases.NewOrderUsecases(app.TxManager)
	if err := app.UserRepository.GetUserByID(c, &user, parsedUserID); err != nil {
		return err
	}
	priceInDecimal, _ := decimal.NewFromString(req.Price)
	qtyInDecimal, _ := decimal.NewFromString(req.Qty)
	if err := orderUsecases.SubmitOrder(c, app.OrderMatchEngine, app.EngineEventProducer, user.ID.String(), req.Symbol, entities.OrderType(req.OrderType), entities.OrderSide(req.Side), priceInDecimal, qtyInDecimal); err != nil {
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
	return nil
}

// TODO : is using the price feed as the price argument in a market order a best practice for this?
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
func PlaceMarketOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	var req schema.PlaceOrderRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	userID, exist := c.Get("userID")
	if !exist {
		zap.S().Infow("userID missing in context")
	}
	parsedUserID, err := uuid.Parse(userID.(string))
	if err != nil {
		responseHelper.UnauthorizedStandard(c, "invalid user ID in token")
		return err
	}

	baseUrl, quoteUrl := strings.Split(req.Symbol, "-")[0], strings.Split(req.Symbol, "-")[1]
	orderUsecases := order_usercases.NewOrderUsecases(*&app.TxManager)
	if err := app.UserRepository.GetUserByID(c, &user, parsedUserID); err != nil {
		return err
	}
	qtyInDecimal, _ := decimal.NewFromString(req.Qty)
	marketPriceOfThePair, err := app.MarketPriceFeed.GetPrice(baseUrl, quoteUrl)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, "price feed interrupted")
		return err
	}

	if err := orderUsecases.SubmitOrder(
		c,
		app.OrderMatchEngine,
		app.EngineEventProducer,
		user.ID.String(),
		req.Symbol,
		entities.OrderTypeMarket,
		entities.OrderSide(req.Side),
		marketPriceOfThePair, // market price for market order
		qtyInDecimal,
	); err != nil {
		return err
	}

	zap.S().Infow("Market order placed",
		"userID", user.ID.String(),
		"symbol", req.Symbol,
		"orderType", "market",
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
func CancelOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	var req schema.CancelOrderRequest
	responseHelper := helper.NewResponseHelper()

	if err := c.ShouldBindJSON(&req); err != nil {
		zap.S().Error("Failed to bind cancel order request: ", err)
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	userID, exist := c.Get("userID")
	if !exist {
		zap.S().Infow("userID missing in context")
	}
	parsedUserID, err := uuid.Parse(userID.(string))
	if err != nil {
		responseHelper.UnauthorizedStandard(c, "invalid user ID in token")
		return err
	}
	if err := app.UserRepository.GetUserByID(c, &user, parsedUserID); err != nil {
		zap.S().Error("Failed to get user by ID: ", err)
		return err
	}
	orderUsecases := order_usercases.NewOrderUsecases(app.TxManager)
	if err := orderUsecases.CancelLimitOrder(c, app.OrderMatchEngine, app.EngineEventProducer, user.ID.String(), req.Symbol, entities.OrderTypeCancel, entities.OrderSide(req.Side), decimal.Zero, decimal.Zero); err != nil {
		zap.S().Error("Failed to cancel order: ", err)
		return err
	}

	responseHelper.SuccessStandard(c, helper.SuccessResponse{
		Success: true,
		Message: "cancel order request sent",
	})
	return nil
}
