package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/domain/entities"
	order_usercases "coinhub/internal/usecases/order"

	"github.com/gin-gonic/gin"
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
	username, exist := c.Get("username")
	if !exist {
		zap.S().Infow("username missing in context")
	}
	zap.S().Infow("username from context", "username", username)

	orderUsecases := order_usercases.NewOrderUsecases(*&app.TxManager)
	if err := app.UserRepository.GetUserByUsername(c, &user, username.(string)); err != nil {
		return err
	}
	priceInDecimal, _ := decimal.NewFromString(req.Price)
	qtyInDecimal, _ := decimal.NewFromString(req.Qty)
	if err := orderUsecases.SubmitOrder(c, app.OrderMatchEngine, app.EngineEventProducer, user.ID.String(), req.Symbol, entities.OrderType(req.OrderType), entities.OrderSide(req.Side), priceInDecimal, qtyInDecimal); err != nil {
		return err
	}

	zap.S().Infow("Limit order placed",
		"username", username,
		"userID", user.ID.String(),
		"symbol", req.Symbol,
		"orderType", req.OrderType,
		"side", req.Side,
		"price", req.Price,
		"qty", req.Qty,
	)
	return nil
}

func PlaceMarketOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	return nil
}

func CancelOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	return nil
}
