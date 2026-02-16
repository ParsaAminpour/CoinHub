package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/services"
	"coinhub/internal/infrastructure/security"
	"coinhub/internal/usecases/user_usecases"
	"fmt"
	"net/http"

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

	registerUsecases := user_usecases.NewRegisterUserUsecases(app.UserRepository, app.WalletAccountRepository)
	hashedUserPassword, err := services.GetUserPasswordHash(req.Password)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	user := entities.NewUser(req.Firstname, req.Lastname, req.Username, req.Gmail, hashedUserPassword, entities.GmailVerificationNotRegistered, entities.StatusActive)
	if err := registerUsecases.Register(c, app.WalletService, user); err != nil {
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	zap.S().Infow("RegisterUser operated successfully")

	// TODO : we should not give the user token here, remove this after gmail verification implemented
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

// LoginUserWithUsername godoc
// @Summary      Login user with username
// @Description  Authenticate user using username and password, returns JWT token
// @Tags         auth
// @Accept       json
// @Produce      json
// @Param        request  body      schema.LoginUserWithUsernameRequest  true  "Login credentials"
// @Success      200      {object}  schema.LoginUserResponse              "User successfully logged in"
// @Failure      400      {object}  helper.ErrorResponse                  "Invalid request body"
// @Failure      401      {object}  helper.ErrorResponse                  "Invalid credentials"
// @Failure      500      {object}  helper.ErrorResponse                  "Internal server error"
// @Router       /v1/auth/login/username [post]
func LoginUserWithUsername(c *gin.Context, app *internal.Application) error {
	var req schema.LoginUserWithUsernameRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	user, err := app.UserRepository.GetUserByUsername(c, req.Username)
	if err != nil {
		responseHelper.UnauthorizedStandard(c, err.Error())
		return err
	}
	passwordVerification, err := services.VerifyUserPasswordHash(req.Password, *user.Password)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, helper.MsgInternalError)
		return err
	}
	if !passwordVerification {
		responseHelper.UnauthorizedStandard(c, "password is wrong")
		return fmt.Errorf("password is wrong")
	}

	jwtToken, err := security.GenerateToken(req.Username)
	if err != nil {
		return err
	}
	zap.S().Infow("LoginUserWithUsername operated successfully", "user", user.ID)

	responseHelper.SuccessStandard(c, schema.LoginUserResponse{
		Code:     http.StatusOK,
		Message:  "user logged in with username successfully",
		JWTToken: jwtToken,
	})
	return nil
}

// LoginUserWithGmail godoc
// @Summary      Login user with gmail
// @Description  Authenticate user using gmail and password, returns JWT token
// @Tags         auth
// @Accept       json
// @Produce      json
// @Param        request  body      schema.LoginWithGmailRequest  true  "Login credentials"
// @Success      200      {object}  schema.LoginUserResponse      "User successfully logged in"
// @Failure      400      {object}  helper.ErrorResponse         "Invalid request body"
// @Failure      401      {object}  helper.ErrorResponse         "Invalid credentials"
// @Failure      500      {object}  helper.ErrorResponse         "Internal server error"
// @Router       /v1/auth/login/gmail [post]
func LoginUserWithGmail(c *gin.Context, app *internal.Application) error {
	var req schema.LoginWithGmailRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	user, err := app.UserRepository.GetUserByGmail(c, req.Gmail)
	if err != nil {
		responseHelper.UnauthorizedStandard(c, err.Error())
		return err
	}
	passwordVerification, err := services.VerifyUserPasswordHash(req.Password, *user.Password)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, helper.MsgInternalError)
		return err
	}
	if !passwordVerification {
		responseHelper.UnauthorizedStandard(c, "password is wrong")
		return fmt.Errorf("password is wrong")
	}

	jwtToken, err := security.GenerateToken(*user.Username)
	if err != nil {
		return err
	}
	zap.S().Infow("LoginUserWithGmail operated successfully", "user", user.ID)

	responseHelper.SuccessStandard(c, schema.LoginUserResponse{
		Code:     http.StatusOK,
		Message:  "user logged in with gmail successfully",
		JWTToken: jwtToken,
	})
	return nil
}
