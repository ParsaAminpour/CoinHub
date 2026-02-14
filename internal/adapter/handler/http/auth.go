package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/domain/entities"
	"coinhub/internal/infrastructure/security"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// RegisterUser godoc
// @Summary      Register a new user
// @Description  Register a new user with username, firstname, lastname, and gmail
// @Tags         auth
// @Accept       json
// @Produce      json
// @Param        request  body      schema.RegisterUserRequest  true  "User registration data"
// @Success      200      {object}  schema.RegisterUserResponse  "User successfully registered"
// @Failure      400      {object}  helper.ErrorResponse          "Invalid request body"
// @Failure      500      {object}  helper.ErrorResponse          "Internal server error"
// @Router       /v1/auth/register [post]
func RegisterUser(c *gin.Context, app *internal.Application) error {
	var req schema.RegisterUserRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}
	zap.S().Infow("RegisterUser request body validated successfully")

	user := entities.NewUser(req.Firstname, req.Lastname, req.Gmail, entities.GmailVerificationNotRegistered, entities.StatusActive)
	if err := app.UserRepository.Create(c.Request.Context(), user); err != nil {
		zap.S().Errorw("RegisterUser error creating user", "error", err)
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	zap.S().Infow("RegisterUser user created successfully", "user", user.ID)

	jwtToken, err := security.GenerateToken(req.Username)
	if err != nil {
		return err
	}
	zap.S().Infow("user jwt token", "token", jwtToken)

	responseHelper.SuccessStandard(c, schema.RegisterUserResponse{
		ID:        user.ID,
		Firstname: user.UserProfile.Firstname,
		Lastname:  user.UserProfile.Lastname,
		Gmail:     *user.Gmail,
	})
	return nil
}
