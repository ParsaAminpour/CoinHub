package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/services"
	"coinhub/internal/infrastructure/security"
	"coinhub/internal/usecases/user_usecases"
	"context"
	"fmt"
	"net/http"
	"time"

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
func RegisterUserHandler(c *gin.Context, app *internal.Application) error {
	var req schema.RegisterUserRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}
	zap.S().Infow("RegisterUser request body validated successfully")

	registerUsecases := user_usecases.NewRegisterUserUsecases(app.TxManager)
	hashedUserPassword, err := services.GetUserPasswordHash(req.Password)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	zap.S().Infow("RegisterUser", "username", req.Username, "hashed password", hashedUserPassword)

	user := entities.NewUser(req.Firstname, req.Lastname, req.Username, req.Gmail, hashedUserPassword, entities.GmailVerificationNotRegistered, entities.StatusActive)
	if err := registerUsecases.Register(c, app.WalletService, user); err != nil {
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	zap.S().Infow("RegisterUser operated successfully")

	if err := tasks.EnqueueEmailVerificationCodeTask(
		c,
		app.AsynqClient,
		time.Now().GoString(),
		c.ClientIP(),
		"New York, USA", // TODO : implement location discovery via Client IP
		c.Request.UserAgent(),
		fmt.Sprintf("%d", time.Now().Year()),
		*user.Gmail,
		*user.Username,
	); err != nil {
		return err
	}

	// TODO : we should not give the user token here, remove this after gmail verification implemented
	if app.Configs.App.Env == "DEVELOPMENT" {
		jwtToken, err := security.GenerateToken(req.Username)
		if err != nil {
			return err
		}
		zap.S().Infow("user jwt token", "token", jwtToken)
	}

	responseHelper.SuccessStandard(c, schema.RegisterUserResponse{
		ID:        user.ID,
		Username:  *user.Username,
		Firstname: user.UserProfile.Firstname,
		Lastname:  user.UserProfile.Lastname,
		Gmail:     *user.Gmail,
	})
	return nil
}

// VerifyGmailVerificationCode godoc
// @Summary      Verify Gmail Verification Code
// @Description  Verify user's Gmail verification code for registration
// @Tags         auth
// @Accept       json
// @Produce      json
// @Param        request  body      schema.GmailVerificationCodeRequest  true  "Verification code request"
// @Success      200      {object}  schema.GmailVerificationCodeResponse "Verification successful, JWT returned"
// @Failure      400      {object}  helper.ErrorResponse                 "Invalid request body"
// @Failure      401      {object}  helper.ErrorResponse                 "Invalid verification code"
// @Failure      500      {object}  helper.ErrorResponse                 "Internal server error"
// @Router       /v1/auth/verify/gmail-code [post]
func VerifyGmailVerificationCode(c *gin.Context, app *internal.Application) error {
	var req schema.GmailVerificationCodeRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	authGmailCache := cache.NewAuthGmailCache(context.Background(), app.RedisClient, time.Minute)
	cachedCode, err := authGmailCache.GetGmailVerificationCode(c, app.RedisClient, req.Gmail, req.Username)
	if err != nil {
		return err
	}

	if cachedCode == req.VerificationCode {
		responseHelper.UnauthorizedStandard(c, "invalid verification code")
		return fmt.Errorf("invalid verification code")
	}

	jwtToken, err := security.GenerateToken(req.Username)
	if err != nil {
		return err
	}
	zap.S().Infow("user jwt token", "token", jwtToken)

	responseHelper.SuccessStandard(c, schema.GmailVerificationCodeResponse{
		Code:     http.StatusOK,
		Message:  "user verified successfuly",
		JWTToken: jwtToken,
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
func LoginUserWithUsernameHandler(c *gin.Context, app *internal.Application) error {
	var req schema.LoginUserWithUsernameRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	if err := app.UserRepository.GetUserByUsername(c, &user, req.Username); err != nil {
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
func LoginUserWithGmailHandler(c *gin.Context, app *internal.Application) error {
	var req schema.LoginWithGmailRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	if err := app.UserRepository.GetUserByGmail(c, &user, req.Gmail); err != nil {
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
