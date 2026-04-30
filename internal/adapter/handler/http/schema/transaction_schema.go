package schema

import (
	"math/big"
	"regexp"
	"strings"

	"github.com/go-playground/validator/v10"
)

type WithdrawNativeRequest struct {
	AssetOwnerAddress  string `json:"asset_owner_address" binding:"required,walletaddresscheck"`
	DestinationAddress string `json:"destination_address" binding:"required,walletaddresscheck"`
	TokenAddress       string `json:"token_address"       binding:"required,walletaddresscheck"`
	TokenSymbol        string `json:"token_symbol"        binding:"required,token_symbol_check"`
	AmountWei          string `json:"amount"              binding:"required,wei_amount"`
	GasLimitUnit       int    `json:"gas_limit_unit"      binding:"required,gt=0"`
	GasPriceWei        string `json:"gas_price_wei"       binding:"required,wei_amount"`
	ChainId            int    `json:"chain_id"            binding:"required,gt=0"`
	Calldata           string `json:"calldata"            binding:"omitempty,calldata_hex"`
}

type WithdrawNativeResponse struct {
	Code              int    `json:"code"`
	TransactionStatus string `json:"transaction_status"`
}

// WeiAmountCheck validates that a string is a valid positive integer (wei denomination).
var WeiAmountCheck validator.Func = func(fl validator.FieldLevel) bool {
	raw := strings.TrimSpace(fl.Field().String())
	if raw == "" {
		return false
	}
	n := new(big.Int)
	if _, ok := n.SetString(raw, 10); !ok {
		return false
	}
	return n.Sign() > 0
}

// TokenSymbolCheck validates that a token symbol is 2–10 uppercase ASCII letters.
var TokenSymbolCheck validator.Func = func(fl validator.FieldLevel) bool {
	s := strings.TrimSpace(fl.Field().String())
	if len(s) < 2 || len(s) > 10 {
		return false
	}
	re := regexp.MustCompile(`^[A-Z]+$`)
	return re.MatchString(s)
}

// CalldataHexCheck validates that calldata is a 0x-prefixed hex string.
// "0x" alone (no payload) is also accepted for plain ETH transfers.
var CalldataHexCheck validator.Func = func(fl validator.FieldLevel) bool {
	s := strings.TrimSpace(fl.Field().String())
	if s == "" {
		return true // omitempty covers the empty case; this is a belt-and-suspenders guard
	}
	re := regexp.MustCompile(`^0x([0-9a-fA-F]{2})*$`)
	return re.MatchString(s)
}

// ValidateWithdrawNativeRequest is a struct-level validator for cross-field rules.
func ValidateWithdrawNativeRequest(sl validator.StructLevel) {
	req, ok := sl.Current().Interface().(WithdrawNativeRequest)
	if !ok {
		return
	}
	if strings.EqualFold(req.AssetOwnerAddress, req.DestinationAddress) {
		sl.ReportError(
			req.DestinationAddress,
			"destination_address",
			"DestinationAddress",
			"destination_must_differ_from_owner",
			"",
		)
	}
}
