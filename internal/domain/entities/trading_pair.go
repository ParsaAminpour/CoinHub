package entities

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type TradingPairStatus string

const (
	TradingPairStatusActive   TradingPairStatus = "active"
	TradingPairStatusInactive TradingPairStatus = "inactive"
)

// The symbol is BaseAsset/QuoteAsset form in both spot and perp
// Selling Base/Quote is equal to buying Quote/Base, so we don't have direction while the Base and Quote are defining the direction themselves.
type TradingPair struct {
	gorm.Model
	ID                      uuid.UUID `gorm:"type:uuid;primaryKey;default:gen_random_uuid()"`
	BaseAssetID             uuid.UUID `gorm:"type:uuid;not null;index"`
	BaseAsset               Asset     `gorm:"foreignKey:BaseAssetID"`
	QuoteAssetID            uuid.UUID `gorm:"type:uuid;not null;index"`
	QuoteAsset              Asset     `gorm:"foreignKey:QuoteAssetID"`
	MaxLeverage             int
	SzDecimal               int
	TickSize                int
	PairNetworkAvailability TradingPairNetworkAvailability `gorm:"type:jsonb"`
	Status                  TradingPairStatus
}

type TradingPairNetworkAvailability struct {
	Testnet bool `json:"testnet"`
	Mainnet bool `json:"mainnet"`
}

func (TradingPair) TableName() string {
	return "trading_pairs"
}
