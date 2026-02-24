package http

import (
	"coinhub/internal"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func GetHome(c *gin.Context, app *internal.Application) error {
	remain, _ := app.AuthGmailCache.GetGmailVerificationCodeTimeLeft(c, app.RedisClient, "megumianzu00@gmail.com", "david")
	if remain < 0 {
		zap.S().Infow("time remained is negative", "duration", remain)
	} else {
		zap.S().Infow("time remained is positive", "duration", remain)
	}
	cachedCode, err := app.AuthGmailCache.GetGmailVerificationCode(c, app.RedisClient, "megumianzu00@gmail.com", "david")
	if err != nil {
		zap.S().Errorw("failed to get gmail verification code", "error", err)
		return err
	}
	zap.S().Infow("fetched cached gmail verification code", "cachedCode", cachedCode)

	// if err := tasks.EnqueueEmailVerificationCodeTask(
	// 	context.Background(),
	// 	app.AsynqClient,
	// 	"2024-01-01 12:00:00",         // requestTime: mock time
	// 	"192.168.1.1",                 // ip: mock IP
	// 	"New York, USA",               // location: mock location
	// 	"Chrome on Windows",           // device: mock device
	// 	"2026",                        // year: mock year
	// 	"parsa.aminpour.cc@gmail.com", // toMail: mock recipient
	// 	"itzparsa",
	// ); err != nil {
	// 	return err
	// }

	// code, err := authGmailCache.GetGmailVerificationCode(context.Background(), app.RedisClient, "parsa.aminpour.cc@gmail.com", "itzparsa")
	// if err != nil {
	// 	zap.S().Errorw("failed to get gmail verification code", "error", err)
	// 	return err
	// }
	// zap.S().Infof("verification code: %v", code)

	c.JSON(http.StatusOK, gin.H{
		"message": "pong",
	})

	return nil
}
