package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/domain/entities"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"go.uber.org/zap"
)

func RegisterUser(c *gin.Context, app *internal.Application) error {
	var req schema.RegisterUserRequest
	if err := c.ShouldBindWith(&req, binding.Query); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": "Invalid request body",
			"error":   err.Error(),
		})
		return err
	}
	zap.S().Infow("RegisterUser request body validated successfully", "request", req)

	user := entities.NewUser(req.Firstname, req.Lastname, req.Gmail, entities.GmailVerificationNotRegistered, entities.StatusActive)
	if err := app.UserRepository.Create(c.Request.Context(), user); err != nil {
		zap.S().Errorw("RegisterUser error creating user", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "Internal server error",
			"error":   err.Error(),
		})
		return err
	}

	zap.S().Infow("RegisterUser user created successfully", "user", user.ID)
	c.JSON(http.StatusCreated, schema.RegisterUserResponse{
		ID:        user.ID,
		Firstname: user.UserProfile.Firstname,
		Lastname:  user.UserProfile.Lastname,
		Gmail:     *user.Gmail,
	})
	return nil
}
