package entities

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// Trade records a single matched execution between a maker (resting) order
// and a taker (incoming) order. One order fill may produce one Trade row.
type Trade struct {
	ID           string          `gorm:"primaryKey"`
	Pair         string          `gorm:"type:varchar(20);not null;index"` // e.g. "BTC/USDT"
	MakerOrderID string          `gorm:"index"`                           // resting order
	TakerOrderID string          `gorm:"index"`                           // incoming order
	MakerOrder   Order           `gorm:"foreignKey:MakerOrderID;references:ID"`
	TakerOrder   Order           `gorm:"foreignKey:TakerOrderID;references:ID"`
	Price        decimal.Decimal `gorm:"type:numeric;not null"` // execution price
	Quantity     decimal.Decimal `gorm:"type:numeric;not null"` // quantity exchanged
	ExecutedAt   time.Time       `gorm:"not null"`              // when the match happened
	CreatedAt    time.Time       `gorm:"autoCreateTime"`
	UpdatedAt    time.Time       `gorm:"autoUpdateTime"`
	DeletedAt    gorm.DeletedAt  `gorm:"index"`
}

func (Trade) TableName() string {
	return "trades"
}

func (t *Trade) BeforeCreate(tx *gorm.DB) error {
	if t.ID == "" {
		t.ID = uuid.NewString()
	}
	return nil
}

// NormalizePairToSlash ensures the trading pair is stored as "X/Y".
// If input is "X-Y", it will be converted to "X/Y". Any other format returns an error.
func NormalizePairToSlash(pair string) (string, error) {
	p := strings.TrimSpace(pair)
	if p == "" {
		return "", fmt.Errorf("pair is required")
	}

	// Enforce exactly one delimiter and two parts.
	var parts []string
	switch {
	case strings.Count(p, "/") == 1 && strings.Count(p, "-") == 0:
		parts = strings.Split(p, "/")
	case strings.Count(p, "-") == 1 && strings.Count(p, "/") == 0:
		parts = strings.Split(p, "-")
	default:
		return "", fmt.Errorf("invalid pair format %q (expected X/Y or X-Y)", pair)
	}

	base := strings.ToUpper(strings.TrimSpace(parts[0]))
	quote := strings.ToUpper(strings.TrimSpace(parts[1]))
	if base == "" || quote == "" {
		return "", fmt.Errorf("invalid pair format %q (empty symbol)", pair)
	}
	return base + "/" + quote, nil
}

func NewTrade(pair, makerOrderID, takerOrderID string, price, quantity decimal.Decimal, executedAt time.Time) (*Trade, error) {
	normalizedPair, err := NormalizePairToSlash(pair)
	if err != nil {
		return nil, err
	}
	if _, err := uuid.Parse(strings.TrimSpace(makerOrderID)); err != nil {
		return nil, fmt.Errorf("invalid makerOrderID %q: %w", makerOrderID, err)
	}
	if _, err := uuid.Parse(strings.TrimSpace(takerOrderID)); err != nil {
		return nil, fmt.Errorf("invalid takerOrderID %q: %w", takerOrderID, err)
	}
	if !price.IsPositive() {
		return nil, fmt.Errorf("price must be > 0")
	}
	if !quantity.IsPositive() {
		return nil, fmt.Errorf("quantity must be > 0")
	}
	if executedAt.IsZero() {
		return nil, fmt.Errorf("executedAt is required")
	}

	return &Trade{
		Pair:         normalizedPair,
		MakerOrderID: strings.TrimSpace(makerOrderID),
		TakerOrderID: strings.TrimSpace(takerOrderID),
		Price:        price,
		Quantity:     quantity,
		ExecutedAt:   executedAt,
	}, nil
}
