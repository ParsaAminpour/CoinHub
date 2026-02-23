package entities

import (
	"database/sql/driver"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

const SPOT_MAX_DECIMALS int = 8
const PERP_MAX_DECIMALS int = 6

type AssetStatus string
type AssetNetwork string

const (
	AssetStatusActive   AssetStatus = "active"
	AssetStatusInactive AssetStatus = "inactive"
)

const (
	TestnetAsset AssetNetwork = "testnet"
	MainnetAsset AssetNetwork = "mainnet"
)

type Asset struct {
	gorm.Model
	ID                  uuid.UUID `json:"id" gorm:"primaryKey;type:uuid;default:gen_random_uuid()"`
	Name                string    `json:"name"`
	Symbol              string    `json:"symbol"`
	MaxSize             float64
	NetworkAvailability AssetNetworkAvailability `gorm:"type:jsonb"`
	Status              AssetStatus
	AssetAddress        string       `json:"asset_address" gorm:"unique;index;not null"`
	Network             AssetNetwork `json:"network" gorm:"default:testnet"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type AssetNetworkAvailability struct {
	SpotTestnet bool `json:"spot_testnet"`
	SpotMainnet bool `json:"spot_mainnet"`
	PerpTestnet bool `json:"perp_testnet"`
	PerpMainnet bool `json:"perp_mainnet"`
}

func (a AssetNetworkAvailability) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *AssetNetworkAvailability) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, a)
}

func (Asset) TableName() string {
	return "assets"
}
