package schema

import (
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type RegisterUserRequest struct {
	Username  string `json:"username" binding:"required,usernamecheck"`
	Firstname string `json:"firstname" binding:"required,firstnamecheck"`
	Lastname  string `json:"lastname" binding:"required,lastnamecheck"`
	Gmail     string `json:"gmail" binding:"required,emailcheck"`
	Password  string `json:"password" binding:"required"`
}

var UsernameCheck validator.Func = func(f1 validator.FieldLevel) bool {
	field := f1.Field().Interface().(string)
	zap.S().Infow("username field", "value", field)
	return len(field) >= 2 && len(field) <= 20
}

var FirstnameCheck validator.Func = func(f1 validator.FieldLevel) bool {
	field := f1.Field().Interface().(string)
	return len(field) >= 2 && len(field) <= 20
}

var LastnameCheck validator.Func = func(f1 validator.FieldLevel) bool {
	field := f1.Field().Interface().(string)
	return len(field) >= 2 && len(field) <= 20
}

var EmailCheck validator.Func = func(f1 validator.FieldLevel) bool {
	field := f1.Field().Interface().(string)
	return strings.Contains(field, "@") && strings.Contains(field, ".")
}

type RegisterUserResponse struct {
	ID        uuid.UUID `json:"id"`
	Username  string    `json:"username"`
	Firstname string    `json:"firstname"`
	Lastname  string    `json:"lastname"`
	Gmail     string    `json:"gmail"`
}

type LoginUserWithUsernameRequest struct {
	Username string `json:"username" binding:"required,usernamecheck"`
	Password string `json:"password" binding:"required"`
}

type LoginWithGmailRequest struct {
	Gmail    string `json:"gmail" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

type LoginUserResponse struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	JWTToken string `json:"jwt_token"`
}

type GmailVerificationCodeRequest struct {
	Gmail            string `json:"gmail"`
	Username         string `json:"username"`
	VerificationCode string `json:"verification_code"`
}

type GmailVerificationCodeResponse struct {
	Code     int    `json:"code"`
	Message  string `json:"message"`
	JWTToken string `json:"jwt_token"`
}

type GmailVerificationCodeResendRequest struct {
	Gmail    string `json:"gmail"`
	Username string `json:"username"`
}

type GmailVerificationCodeResendResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
