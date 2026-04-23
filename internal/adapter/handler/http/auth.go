package http

import (
	"coinhub/internal"
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/services"
	"coinhub/internal/infrastructure/security"
	"coinhub/internal/usecases/user_usecases"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ipinfo/go/v2/ipinfo"
	"go.uber.org/zap"
	"gorm.io/gorm"
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

	user := entities.NewUser(req.Firstname, req.Lastname, req.Username, req.Gmail, hashedUserPassword, entities.GmailVerificationStatusPending, entities.StatusActive)
	if err := registerUsecases.Register(c, app.WalletService, user); err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			if user.GmailVerificationStatus == entities.GmailVerificationStatusPending {
				responseHelper.BadRequestStandard(c, "user has already registered with pending gmail verification status")
				return err
			}
		}
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}
	zap.S().Infow("RegisterUser operated successfully")

	if app.Configs.App.IPInfoServiceToken == "" {
		zap.S().Fatal("missing IPINFO_TOKEN")
	}

	client := ipinfo.NewLiteClient(nil, nil, app.Configs.App.IPInfoServiceToken)
	details, err := client.GetIPInfo(net.ParseIP(c.ClientIP()))
	if err != nil {
		zap.S().Fatal(err)
	}

	if err := tasks.EnqueueEmailVerificationCodeTask(
		c,
		app.AsynqClient,
		time.Now().Format(time.RFC3339),
		c.ClientIP(),
		details.Country,
		c.Request.UserAgent(),
		fmt.Sprintf("%d", time.Now().Year()),
		*user.Gmail,
		*user.Username,
	); err != nil {
		return err
	}

	if app.Configs.App.Env == "DEVELOPMENT" {
		jwtToken, err := security.GenerateToken(user.ID.String())
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

	zap.S().Infow(
		"Verifying Gmail verification code request",
		"gmail", req.Gmail,
		"username", req.Username,
		"code", req.VerificationCode,
	)

	var user entities.User
	if err := app.UserRepository.GetUserByGmail(c, &user, req.Gmail); err != nil {
		zap.S().Warnw("User not found for resend gmail verification", "gmail", req.Gmail, "error", err)
		responseHelper.UnauthorizedStandard(c, "user not found")
		return fmt.Errorf("user not found")
	}
	if user.Username == nil || strings.TrimSpace(*user.Username) != strings.TrimSpace(req.Username) {
		zap.S().Warnw(
			"requested_username", req.Username,
			"user_username_ptr", user.Username,
		)
		responseHelper.UnauthorizedStandard(c, "username and gmail do not match")
		return fmt.Errorf("username and gmail do not match")
	}

	verifyGmailUsecases := user_usecases.NewVerifyGmailUsecases(app.UserRepository)
	if err := verifyGmailUsecases.LabelGmailVerificationStatus(c, app.RedisClient, app.AuthGmailCache, app.UserRepository, req.Gmail, req.Username, req.VerificationCode, entities.GmailVerificationStatusVerified); err != nil {
		zap.S().Errorw("failed to verify gmail verification code", "error", err.Error())
		responseHelper.UnauthorizedStandard(c, err.Error())
		return err
	}

	jwtToken, err := security.GenerateToken(user.ID.String())
	if err != nil {
		return err
	}

	responseHelper.SuccessStandard(c, schema.GmailVerificationCodeResponse{
		Code:     http.StatusOK,
		Message:  "user verified successfuly",
		JWTToken: jwtToken,
	})
	return nil
}

// ResendGmailVerificationCodeHandler godoc
// @Summary      Resend Gmail Verification Code
// @Description  Resend a new verification code to user's Gmail
// @Tags         auth
// @Accept       json
// @Produce      json
// @Param        request  body      schema.GmailVerificationCodeResendRequest  true  "Resend gmail verification code request"
// @Success      200      {object}  helper.SuccessResponse               "Verification code resent successfully"
// @Failure      400      {object}  helper.ErrorResponse                 "Invalid request body"
// @Failure      401      {object}  helper.ErrorResponse                 "Unauthorized or Gmail not found"
// @Failure      500      {object}  helper.ErrorResponse                 "Internal server error"
// @Router       /v1/auth/resend/gmail-code [post]
func ResendGmailVerificationCodeHandler(c *gin.Context, app *internal.Application) error {
	var req schema.GmailVerificationCodeResendRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	if err := app.UserRepository.GetUserByGmail(c, &user, req.Gmail); err != nil {
		zap.S().Warnw("User not found for resend gmail verification", "gmail", req.Gmail, "error", err)
		responseHelper.UnauthorizedStandard(c, "user not found")
		return fmt.Errorf("user not found")
	}
	if user.Username == nil || strings.TrimSpace(*user.Username) != strings.TrimSpace(req.Username) {
		zap.S().Warnw(
			"requested_username", req.Username,
			"user_username_ptr", user.Username,
		)
		responseHelper.UnauthorizedStandard(c, "username and gmail do not match")
		return fmt.Errorf("username and gmail do not match")
	}

	if user.IsVerified {
		zap.S().Errorln("the user is already verified")
		responseHelper.BadRequestStandard(c, "the user is already verified")
		return fmt.Errorf("the user is already verified")
	}

	ttl, err := app.AuthGmailCache.GetGmailVerificationCodeTimeLeft(c, app.RedisClient, req.Gmail, req.Username)
	if err != nil {
		zap.S().Errorw("failed to get gmail verification code TTL", "error", err, "gmail", req.Gmail, "username", req.Username)
		responseHelper.InternalServerError(c, "failed to check verification code TTL")
		return err
	}
	if ttl.Seconds() > 0 {
		responseHelper.ErrorStandard(c, http.StatusTooEarly, "Verification code has already sent. Please try again later.")
		return fmt.Errorf("verification code for this user is still alive")
	}

	if app.Configs.App.IPInfoServiceToken == "" {
		zap.S().Fatal("missing IPINFO_TOKEN")
	}

	client := ipinfo.NewLiteClient(nil, nil, app.Configs.App.IPInfoServiceToken)
	details, err := client.GetIPInfo(net.ParseIP(c.ClientIP()))
	if err != nil {
		zap.S().Fatal(err)
	}

	// You might enqueue an async mail job here, mock for now.
	if err := tasks.EnqueueEmailVerificationCodeTask(
		c,
		app.AsynqClient,
		time.Now().GoString(),
		c.ClientIP(),
		details.Country,
		c.Request.UserAgent(),
		fmt.Sprintf("%d", time.Now().Year()),
		*user.Gmail,
		*user.Username,
	); err != nil {
		return err
	}

	responseHelper.SuccessStandard(c, gin.H{
		"code":    http.StatusOK,
		"message": "verification code resent successfully",
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

	jwtToken, err := security.GenerateToken(user.ID.String())
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

	jwtToken, err := security.GenerateToken(user.ID.String())
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
