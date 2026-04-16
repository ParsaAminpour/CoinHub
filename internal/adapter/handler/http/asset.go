package http

import (
	"coinhub/internal"

	"github.com/gin-gonic/gin"
)

func CreateAssetAdminOperationHandler(c *gin.Context, app *internal.Application) error {
	// TODO : implement the creation handlers...

	newPairCount, _ := app.TradingPairRepository.GetActivePairsCount(c)
	app.KafkaTopicManager.OnNewPair(c, newPairCount)
	return nil
}
