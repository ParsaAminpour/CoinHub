package schema

import (
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

type RegisterUserRequest struct {
	Firstname string `json:"firstname" binding:"required,FirstnameCheck"`
	Lastname  string `json:"lastname" binding:"required,LastnameCheck"`
	Gmail     string `json:"gmail" binding:"required,EmailCheck"`
}

var FirstnameCheck validator.Func = func(f1 validator.FieldLevel) bool {
	field := f1.Field().Interface().(string)
	return len(field) >= 2 && len(field) <= 100
}

var LastnameCheck validator.Func = func(f1 validator.FieldLevel) bool {
	field := f1.Field().Interface().(string)
	return len(field) >= 2 && len(field) <= 100
}

var EmailCheck validator.Func = func(f1 validator.FieldLevel) bool {
	field := f1.Field().Interface().(string)
	return strings.Contains(field, "@") && strings.Contains(field, ".")
}

type RegisterUserResponse struct {
	ID        uuid.UUID `json:"id"`
	Firstname string    `json:"firstname"`
	Lastname  string    `json:"lastname"`
	Gmail     string    `json:"gmail"`
}
