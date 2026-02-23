package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/tasks"
	"net/http"

	"github.com/gin-gonic/gin"
)

func GetHome(c *gin.Context, app *internal.Application) error {
	tasks.EnqueuPendingTransactions(c, app.AsynqClient, "0x496921b0a50bd4f2f1eb0ce07d861e66fae47cf5bcf07028417f92e56d819b5c", 11155111)
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})

	return nil
}
