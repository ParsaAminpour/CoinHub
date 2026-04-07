package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"

	"github.com/gin-gonic/gin"
)

func PlaceLimitOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	var req schema.PlaceOrderRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	return nil
}

func PlaceMarketOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	return nil
}

func CancelOrderHTTPHandler(c *gin.Context, app *internal.Application) error {
	return nil
}
