package market

import (
	"strings"

	"github.com/shopspring/decimal"
)

// PriceFeed is the single interface the rest of the application uses.
// GetPrice accepts the base and quote currency symbols (e.g. "BTC", "USDT")
// and returns the current market price or an error.
type PriceFeed interface {
	GetPrice(baseSym, quoteSym string) (decimal.Decimal, error)
}

// NewPriceFeed constructs the appropriate PriceFeed implementation based on
// providerName. baseURL is the API root (e.g. "https://api.coingecko.com/api/v3")
// and apiKey is the provider token stored in configuration.
func NewPriceFeed(providerName, baseURL, apiKey string) PriceFeed {
	switch strings.ToLower(strings.TrimSpace(providerName)) {
	case "coingecko":
		return NewCoinGeckoPriceFeed(baseURL, apiKey)
	default:
		return NoopPriceFeed{}
	}
}

// NoopPriceFeed is a safe no-op used when no provider is configured.
type NoopPriceFeed struct{}

func (NoopPriceFeed) GetPrice(_, _ string) (decimal.Decimal, error) { return decimal.Zero, nil }
