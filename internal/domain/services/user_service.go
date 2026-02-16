package services

import "coinhub/internal/infrastructure/security"

// The buissiness logic for the User entity, like validating and hashing password, NO DB CALL!
func GetUserPasswordHash[T interface{ string | []byte }](rawPassword T) (string, error) {
	return security.CreateHash(rawPassword, security.DefaultParams)
}

func VerifyUserPasswordHash[T interface{ string | []byte }](rawPassword T, passwordHash string) (bool, error) {
	return security.ComparePasswordAndHash(rawPassword, passwordHash)
}
