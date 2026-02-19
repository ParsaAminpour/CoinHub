package validators

import (
	"regexp"

	"github.com/go-playground/validator/v10"
)

var WalletAddressCheck validator.Func = func(f1 validator.FieldLevel) bool {
	walletAddress := f1.Field().Interface().(string)
	re := regexp.MustCompile("^0x[0-9a-fA-F]{40}$")
	return re.MatchString(walletAddress)
}
