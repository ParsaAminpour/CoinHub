package services

import (
	"coinhub/internal/infrastructure/security"
	"crypto/rand"
	"math/big"
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
	result := make([]byte, length)

	for i := range result {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return ""
		}
		result[i] = charset[n.Int64()]
	}

	return string(result)
}
