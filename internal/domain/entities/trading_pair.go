package entities

import (
	"database/sql/driver"
	"encoding/json"

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

func (t TradingPairNetworkAvailability) Value() (driver.Value, error) {
	return json.Marshal(t)
}

func (t *TradingPairNetworkAvailability) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, t)
}

func (TradingPair) TableName() string {
	return "trading_pairs"
}

// Symbol returns the trading pair in BASE/QUOTE format (e.g. "BTC/USDT").
// Requires BaseAsset and QuoteAsset to be preloaded.
func (tp TradingPair) Symbol() string {
	return *tp.BaseAsset.Symbol + "/" + *tp.QuoteAsset.Symbol
}
