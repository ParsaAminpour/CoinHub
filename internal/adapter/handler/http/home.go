package http

import (
	"coinhub/internal"
	"net/http"

	"github.com/gin-gonic/gin"
)

func GetHome(c *gin.Context, app *internal.Application) error {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
	return nil
}
