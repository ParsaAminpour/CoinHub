package http

import (
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/domain/services"
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/security"
	"coinhub/internal/usecases/user_usecases"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	"github.com/ipinfo/go/v2/ipinfo"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type AuthHandler struct {
	TxManager      *repositories.TxManager
	WalletService  *services.WalletService
	UserRepository repositories.UserRepository
	RedisClient    *redis.Client
	AuthGmailCache *cache.AuthGmailCache
	AsynqClient    *asynq.Client
	Configs        configs.Configuration
}

func NewAuthHandler(
	txManager *repositories.TxManager,
	walletService *services.WalletService,
	userRepository repositories.UserRepository,
	redisClient *redis.Client,
	authGmailCache *cache.AuthGmailCache,
	asynqClient *asynq.Client,
	configs configs.Configuration,
) HttpAPIHandler {
	return &AuthHandler{
		TxManager:      txManager,
		WalletService:  walletService,
		UserRepository: userRepository,
		RedisClient:    redisClient,
		AuthGmailCache: authGmailCache,
		AsynqClient:    asynqClient,
		Configs:        configs,
	}
}

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
func RegisterUserHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*AuthHandler)
	if !ok {
		return errors.New("invalid handler context for RegisterUserHandler")
	}
	var req schema.RegisterUserRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}
	zap.S().Infow("RegisterUser request body validated successfully")

	registerUsecases := user_usecases.NewRegisterUserUsecases(*h.TxManager)
	hashedUserPassword, err := services.GetUserPasswordHash(req.Password)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, err.Error())
		return err
	}

	user := entities.NewUser(req.Firstname, req.Lastname, req.Username, req.Gmail, hashedUserPassword, entities.GmailVerificationStatusPending, entities.StatusActive)
	if err := registerUsecases.Register(c, *h.WalletService, user); err != nil {
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

	if h.Configs.App.IPInfoServiceToken == "" {
		zap.S().Fatal("missing IPINFO_TOKEN")
	}
	client := ipinfo.NewLiteClient(nil, nil, h.Configs.App.IPInfoServiceToken)
	details, err := client.GetIPInfo(net.ParseIP(c.ClientIP()))
	if err != nil {
		zap.S().Fatal(err)
	}

	if err := tasks.EnqueueEmailVerificationCodeTask(
		c, h.AsynqClient,
		time.Now().Format(time.RFC3339),
		c.ClientIP(), details.Country,
		c.Request.UserAgent(),
		fmt.Sprintf("%d", time.Now().Year()),
		*user.Gmail, *user.Username,
	); err != nil {
		return err
	}

	if h.Configs.App.Env == "DEVELOPMENT" {
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
func VerifyGmailVerificationCode(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*AuthHandler)
	if !ok {
		return errors.New("invalid handler context for VerifyGmailVerificationCode")
	}
	var req schema.GmailVerificationCodeRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	zap.S().Infow("Verifying Gmail verification code request",
		"gmail", req.Gmail, "username", req.Username, "code", req.VerificationCode)

	var user entities.User
	if err := h.UserRepository.GetUserByGmail(c, &user, req.Gmail); err != nil {
		zap.S().Warnw("User not found for gmail verification", "gmail", req.Gmail, "error", err)
		responseHelper.UnauthorizedStandard(c, "user not found")
		return fmt.Errorf("user not found")
	}
	if user.Username == nil || strings.TrimSpace(*user.Username) != strings.TrimSpace(req.Username) {
		responseHelper.UnauthorizedStandard(c, "username and gmail do not match")
		return fmt.Errorf("username and gmail do not match")
	}

	verifyGmailUsecases := user_usecases.NewVerifyGmailUsecases(h.UserRepository)
	if err := verifyGmailUsecases.LabelGmailVerificationStatus(c, h.RedisClient, h.AuthGmailCache, h.UserRepository, req.Gmail, req.Username, req.VerificationCode, entities.GmailVerificationStatusVerified); err != nil {
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
func ResendGmailVerificationCodeHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*AuthHandler)
	if !ok {
		return errors.New("invalid handler context for ResendGmailVerificationCodeHandler")
	}
	var req schema.GmailVerificationCodeResendRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	if err := h.UserRepository.GetUserByGmail(c, &user, req.Gmail); err != nil {
		zap.S().Warnw("User not found for resend gmail verification", "gmail", req.Gmail, "error", err)
		responseHelper.UnauthorizedStandard(c, "user not found")
		return fmt.Errorf("user not found")
	}
	if user.Username == nil || strings.TrimSpace(*user.Username) != strings.TrimSpace(req.Username) {
		responseHelper.UnauthorizedStandard(c, "username and gmail do not match")
		return fmt.Errorf("username and gmail do not match")
	}
	if user.IsVerified {
		responseHelper.BadRequestStandard(c, "the user is already verified")
		return fmt.Errorf("the user is already verified")
	}

	ttl, err := h.AuthGmailCache.GetGmailVerificationCodeTimeLeft(c, h.RedisClient, req.Gmail, req.Username)
	if err != nil {
		zap.S().Errorw("failed to get gmail verification code TTL", "error", err)
		responseHelper.InternalServerError(c, "failed to check verification code TTL")
		return err
	}
	if ttl.Seconds() > 0 {
		responseHelper.ErrorStandard(c, http.StatusTooEarly, "Verification code has already sent. Please try again later.")
		return fmt.Errorf("verification code for this user is still alive")
	}

	if h.Configs.App.IPInfoServiceToken == "" {
		zap.S().Fatal("missing IPINFO_TOKEN")
	}
	client := ipinfo.NewLiteClient(nil, nil, h.Configs.App.IPInfoServiceToken)
	details, err := client.GetIPInfo(net.ParseIP(c.ClientIP()))
	if err != nil {
		zap.S().Fatal(err)
	}

	if err := tasks.EnqueueEmailVerificationCodeTask(
		c, h.AsynqClient,
		time.Now().GoString(),
		c.ClientIP(), details.Country,
		c.Request.UserAgent(),
		fmt.Sprintf("%d", time.Now().Year()),
		*user.Gmail, *user.Username,
	); err != nil {
		return err
	}

	responseHelper.SuccessStandard(c, gin.H{
		"code":    http.StatusOK,
		"message": "verification code resent successfully",
	})
	return nil
}

// LoginUserWithUsernameHandler godoc
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
func LoginUserWithUsernameHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*AuthHandler)
	if !ok {
		return errors.New("invalid handler context for LoginUserWithUsernameHandler")
	}
	var req schema.LoginUserWithUsernameRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	if err := h.UserRepository.GetUserByUsername(c, &user, req.Username); err != nil {
		responseHelper.UnauthorizedStandard(c, err.Error())
		return err
	}

	ok2, err := services.VerifyUserPasswordHash(req.Password, *user.Password)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, helper.MsgInternalError)
		return err
	}
	if !ok2 {
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

// MockLoginHandler — DEVELOPMENT only.
// Finds or creates a simulation user by username and returns a JWT without password verification.
// Used by the simulate_users.sh script to obtain tokens without going through email verification.
func MockLoginHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*AuthHandler)
	if !ok {
		return errors.New("invalid handler context for MockLoginHandler")
	}
	responseHelper := helper.NewResponseHelper()

	username := "simuser"
	var body struct {
		Username string `json:"username"`
	}
	if err := c.ShouldBindJSON(&body); err == nil && body.Username != "" {
		username = body.Username
	}

	var user entities.User
	if err := h.UserRepository.GetUserByUsername(c, &user, username); err != nil {
		// User not found — create a dev sim user (no email verification needed)
		hashedPw, hashErr := services.GetUserPasswordHash("SimPass123!")
		if hashErr != nil {
			responseHelper.InternalServerErrorStandard(c, hashErr.Error())
			return hashErr
		}
		newUser := entities.NewUser("Sim", "User", username, username+"@coinhub.dev", hashedPw, entities.GmailVerificationStatusVerified, entities.StatusActive)
		newUser.IsVerified = true
		registerUC := user_usecases.NewRegisterUserUsecases(*h.TxManager)
		if createErr := registerUC.Register(c, *h.WalletService, newUser); createErr != nil {
			responseHelper.InternalServerErrorStandard(c, createErr.Error())
			return createErr
		}
		user = *newUser
	}

	token, tokenErr := security.GenerateToken(user.ID.String())
	if tokenErr != nil {
		return tokenErr
	}

	responseHelper.SuccessStandard(c, schema.LoginUserResponse{
		Code:     http.StatusOK,
		Message:  "mock login successful",
		JWTToken: token,
	})
	return nil
}

// LoginUserWithGmailHandler godoc
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
func LoginUserWithGmailHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*AuthHandler)
	if !ok {
		return errors.New("invalid handler context for LoginUserWithGmailHandler")
	}
	var req schema.LoginWithGmailRequest
	responseHelper := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		responseHelper.InvalidRequestBody(c)
		return err
	}

	var user entities.User
	if err := h.UserRepository.GetUserByGmail(c, &user, req.Gmail); err != nil {
		responseHelper.UnauthorizedStandard(c, err.Error())
		return err
	}

	ok2, err := services.VerifyUserPasswordHash(req.Password, *user.Password)
	if err != nil {
		responseHelper.InternalServerErrorStandard(c, helper.MsgInternalError)
		return err
	}
	if !ok2 {
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
