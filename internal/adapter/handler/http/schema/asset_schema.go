package schema

import (
	"coinhub/internal/domain/entities"

	"github.com/google/uuid"
)

type CreateAssetRequest struct {
	Name                string                          `json:"name"                 binding:"required"`
	Symbol              string                          `json:"symbol"               binding:"required,token_symbol_check"`
	AssetAddress        string                          `json:"asset_address"        binding:"required,walletaddresscheck"`
	Network             entities.AssetNetwork           `json:"network"              binding:"required,oneof=testnet mainnet"`
	NetworkAvailability entities.AssetNetworkAvailability `json:"network_availability" binding:"required"`
	MaxSize             float64                         `json:"max_size"             binding:"required,gt=0"`
}

type CreateAssetResponse struct {
	ID                  uuid.UUID                       `json:"id"`
	Name                string                          `json:"name"`
	Symbol              string                          `json:"symbol"`
	AssetAddress        string                          `json:"asset_address"`
	Network             entities.AssetNetwork           `json:"network"`
	NetworkAvailability entities.AssetNetworkAvailability `json:"network_availability"`
	MaxSize             float64                         `json:"max_size"`
	Status              entities.AssetStatus            `json:"status"`
}
