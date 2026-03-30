package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/tasks"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func GetHome(c *gin.Context, app *internal.Application) error {
	if err := tasks.EnqueueTransferEventTask(
		c,
		app.AsynqClient,
		"0x64ff606c52067dfa870db3b5700ed4c63c10df1ce72c0f1283887aa2b8f52cab",
		"0x1c255DB352E8B3CC16EFd721C61d7b1B5952b2bb",
		"0xc7d1cd54dcb800919614e6b1f073941b57edfc3e",
		"1000000000",
		"0x036CbD53842c5426634e7929541eC2318f3dCF7e",
		38131927,
	); err != nil {
		zap.S().Errorf("failed to enqueue transfer event task: %v", err)
		return err
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})

	return nil
}
