package helper

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type ResponseHelper struct{}

func NewResponseHelper() *ResponseHelper {
	return &ResponseHelper{}
}

func (rh *ResponseHelper) Success(c *gin.Context, data interface{}, message ...string) {
	c.JSON(http.StatusOK, NewSuccessResponse(data, message...))
}

func (rh *ResponseHelper) SuccessWithMeta(c *gin.Context, data interface{}, meta interface{}, message ...string) {
	c.JSON(http.StatusOK, NewSuccessResponseWithMeta(data, meta, message...))
}

func (rh *ResponseHelper) ValidationError(c *gin.Context, errors interface{}) {
	c.JSON(http.StatusUnprocessableEntity, NewValidationErrorResponse(errors))
}

func (rh *ResponseHelper) BadRequest(c *gin.Context, message string, errors ...interface{}) {
	var err interface{}
	if len(errors) > 0 {
		err = errors[0]
	}
	c.JSON(http.StatusBadRequest, NewErrorResponse(message, err))
}

func (rh *ResponseHelper) Unauthorized(c *gin.Context, message string, errors ...interface{}) {
	var err interface{}
	if len(errors) > 0 {
		err = errors[0]
	}
	c.JSON(http.StatusUnauthorized, NewErrorResponse(message, err))
}

func (rh *ResponseHelper) Forbidden(c *gin.Context, message string, errors ...interface{}) {
	var err interface{}
	if len(errors) > 0 {
		err = errors[0]
	}
	c.JSON(http.StatusForbidden, NewErrorResponse(message, err))
}

func (rh *ResponseHelper) NotFound(c *gin.Context, message string, errors ...interface{}) {
	var err interface{}
	if len(errors) > 0 {
		err = errors[0]
	}
	c.JSON(http.StatusNotFound, NewErrorResponse(message, err))
}

func (rh *ResponseHelper) Conflict(c *gin.Context, message string, errors ...interface{}) {
	var err interface{}
	if len(errors) > 0 {
		err = errors[0]
	}
	c.JSON(http.StatusConflict, NewErrorResponse(message, err))
}

func (rh *ResponseHelper) UnprocessableEntity(c *gin.Context, message string, errors interface{}) {
	c.JSON(http.StatusUnprocessableEntity, NewErrorResponse(message, errors))
}

func (rh *ResponseHelper) InternalServerError(c *gin.Context, message ...string) {
	msg := MsgInternalError
	if len(message) > 0 && message[0] != "" {
		msg = message[0]
	}
	c.JSON(http.StatusInternalServerError, NewErrorResponse(msg, nil))
}

func (rh *ResponseHelper) ServiceUnavailable(c *gin.Context, message string) {
	c.JSON(http.StatusServiceUnavailable, NewErrorResponse(message, nil))
}

func (rh *ResponseHelper) InvalidRequestBody(c *gin.Context) {
	c.JSON(http.StatusBadRequest, NewErrorResponse(MsgInvalidRequestBody, nil))
}

func (rh *ResponseHelper) TokenGenerationFailed(c *gin.Context) {
	c.JSON(http.StatusInternalServerError, NewErrorResponse(MsgTokenGenerationFailed, nil))
}

func (rh *ResponseHelper) InvalidToken(c *gin.Context) {
	c.JSON(http.StatusUnauthorized, NewErrorResponse(MsgInvalidToken, nil))
}

func (rh *ResponseHelper) InvalidCredentials(c *gin.Context) {
	c.JSON(http.StatusUnauthorized, NewErrorResponse(MsgInvalidCredentials, nil))
}

func (rh *ResponseHelper) TokenExpired(c *gin.Context) {
	c.JSON(http.StatusUnauthorized, NewErrorResponse(MsgTokenExpired, nil))
}

func (rh *ResponseHelper) SuccessStandard(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, data)
}

func (rh *ResponseHelper) ErrorStandard(c *gin.Context, statusCode int, message string) {
	c.JSON(statusCode, gin.H{
		"code":    statusCode,
		"message": message,
	})
}

func (rh *ResponseHelper) BadRequestStandard(c *gin.Context, message string) {
	rh.ErrorStandard(c, http.StatusBadRequest, message)
}

func (rh *ResponseHelper) UnauthorizedStandard(c *gin.Context, message string) {
	rh.ErrorStandard(c, http.StatusUnauthorized, message)
}

func (rh *ResponseHelper) InternalServerErrorStandard(c *gin.Context, message string) {
	rh.ErrorStandard(c, http.StatusInternalServerError, message)
}

func (rh *ResponseHelper) NotFoundStandard(c *gin.Context, message string) {
	rh.ErrorStandard(c, http.StatusNotFound, message)
}

// Response types
type SuccessResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data"`
}

type SuccessResponseWithMeta struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data"`
	Meta    interface{} `json:"meta"`
}

type ErrorResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Error   interface{} `json:"error,omitempty"`
}

type ValidationErrorResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Errors  interface{} `json:"errors"`
}

// Helper functions
func NewSuccessResponse(data interface{}, message ...string) SuccessResponse {
	msg := "Operation successful"
	if len(message) > 0 && message[0] != "" {
		msg = message[0]
	}
	return SuccessResponse{
		Success: true,
		Message: msg,
		Data:    data,
	}
}

func NewSuccessResponseWithMeta(data interface{}, meta interface{}, message ...string) SuccessResponseWithMeta {
	msg := "Operation successful"
	if len(message) > 0 && message[0] != "" {
		msg = message[0]
	}
	return SuccessResponseWithMeta{
		Success: true,
		Message: msg,
		Data:    data,
		Meta:    meta,
	}
}

func NewErrorResponse(message string, err interface{}) ErrorResponse {
	return ErrorResponse{
		Success: false,
		Message: message,
		Error:   err,
	}
}

func NewValidationErrorResponse(errors interface{}) ValidationErrorResponse {
	return ValidationErrorResponse{
		Success: false,
		Message: "Validation failed",
		Errors:  errors,
	}
}

// Message constants
const (
	MsgInternalError         = "Internal server error"
	MsgInvalidRequestBody    = "Invalid request body"
	MsgTokenGenerationFailed = "Token generation failed"
	MsgInvalidToken          = "Invalid token"
	MsgInvalidCredentials    = "Invalid credentials"
	MsgTokenExpired          = "Token expired"
)
