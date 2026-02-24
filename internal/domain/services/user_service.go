package services

import (
	"coinhub/internal/infrastructure/security"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

// The buissiness logic for the User entity, like validating and hashing password, NO DB CALL!
func GetUserPasswordHash[T interface{ string | []byte }](rawPassword T) (string, error) {
	return security.CreateHash(rawPassword, security.DefaultParams)
}

func VerifyUserPasswordHash[T interface{ string | []byte }](rawPassword T, passwordHash string) (bool, error) {
	return security.ComparePasswordAndHash(rawPassword, passwordHash)
}

func GenerateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	zap.S().Debugw(" +++ code", "code", strings.ToUpper(string(b)))
	return strings.ToUpper(string(b))
}
