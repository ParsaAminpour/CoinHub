package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func GetHome(c *gin.Context, db *gorm.DB) error {
	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})
	return nil
}
