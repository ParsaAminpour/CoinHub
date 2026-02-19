package schema

type WithdrawNativeRequest struct {
	AssetOwnerAddress  string `json:"asset_owner_address" binding:"required,walletaddresscheck"`
	DestinationAddress string `json:"destination_address" binding:"required,walletaddresscheck"`
	TokenAddress       string `json:"token_address" binding:"required,walletaddresscheck"`
	TokenSymbol        string `json:"token_symbol" binding:"required"`
	AmountWei          string `json:"amount" binding:"required"`
	GasLimitUnit       int    `json:"gas_limit_unit" binding:"required,gt=0"`
	GasPriceWei        string `json:"gas_price_wei" binding:"required"`
	ChainId            int    `json:"chain_id" binding:"required"`
	Calldata           string `json:"calldata"`
}

type WithdrawNativeResponse struct {
	Code              int    `json:"code"`
	TransactionStatus string `json:"transaction_status"`
}
