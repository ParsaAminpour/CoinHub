package schema

import (
	"math/big"
	"regexp"

	"github.com/go-playground/validator/v10"
)

type GetWalletETHBalance struct {
	WalletAddress string `json:"wallet_address" binding:"required,walletaddresscheck"`
}

type GetWalletETHBalanceResponse struct {
	Code    int     `json:"code"`
	Balance big.Int `json:"balance"`
}

var WalletAddressCheck validator.Func = func(f1 validator.FieldLevel) bool {
	walletAddress := f1.Field().Interface().(string)
	re := regexp.MustCompile("^0x[0-9a-fA-F]{40}$")
	return re.MatchString(walletAddress)
}
