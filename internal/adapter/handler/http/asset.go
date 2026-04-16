package http

import (
	"coinhub/internal"

	"github.com/gin-gonic/gin"
)

func CreateAssetAdminOperationHandler(c *gin.Context, app *internal.Application) error {
	// TODO : implement the creation handlers...

	newAssetsCount, _ := app.AssetRepository.GetActiveTradingPairsCount(c)
	app.KafkaTopicManager.OnNewPair(c, newAssetsCount)
	return nil
}
