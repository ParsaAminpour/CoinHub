package entities

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type OrderType string
type OrderSide string
type OrderStatus string

const (
	OrderTypeLimit  OrderType = "limit"
	OrderTypeMarket OrderType = "market"
	OrderTypeCancel OrderType = "cancel"

	SideBuy  OrderSide = "buy"
	SideSell OrderSide = "sell"

	StatusOpen      OrderStatus = "open"
	StatusPartial   OrderStatus = "partial"
	StatusFilled    OrderStatus = "filled"
	StatusCancelled OrderStatus = "cancelled"
)

type Order struct {
	ID        string          `gorm:"type:uuid;primaryKey;default:gen_random_uuid()"` // Unique order identifier
	UserID    string          `gorm:"type:uuid;not null;index"`                       // User that placed the order
	User      User            `gorm:"foreignKey:UserID"`
	Pair      string          `gorm:"type:varchar(20);not null;index"`          // Trading pair, e.g., "BTC/USDT"
	Type      OrderType       `gorm:"type:varchar(10);not null"`                // Order type: limit, market, cancel
	Side      OrderSide       `gorm:"type:varchar(10);not null"`                // Order side: buy or sell
	Price     decimal.Decimal `gorm:"type:numeric;default:0"`                   // Order price (0 for market orders)
	Quantity  decimal.Decimal `gorm:"type:numeric;not null"`                    // Order quantity
	Filled    decimal.Decimal `gorm:"type:numeric;default:0"`                   // Quantity filled so far
	Status    OrderStatus     `gorm:"type:varchar(15);not null;default:'open'"` // Order status
	Timestamp time.Time       `gorm:"autoCreateTime:milli"`                     // Order creation or placement time
	CreatedAt time.Time       `gorm:"autoCreateTime"`
	UpdatedAt time.Time       `gorm:"autoUpdateTime"`
	DeletedAt gorm.DeletedAt  `gorm:"index"`
}

func validateNewOrderInputs(
	userID string,
	pair string,
	orderType OrderType,
	side OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal,
) error {
	if userID == "" {
		return fmt.Errorf("userID is required")
	}
	if pair == "" || !strings.Contains(pair, "/") {
		return fmt.Errorf("pair is required")
	}
	if orderType != OrderTypeLimit && orderType != OrderTypeMarket && orderType != OrderTypeCancel {
		return fmt.Errorf("invalid order type: %s", orderType)
	}
	if side != SideBuy && side != SideSell {
		return fmt.Errorf("invalid order side: %s", side)
	}
	if price.IsNegative() {
		return fmt.Errorf("price must not be negative")
	}
	if quantity.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("quantity must be positive")
	}
	return nil
}

func NewOrder(
	userID string,
	pair string,
	orderType OrderType,
	side OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal,
) *Order {
	if err := validateNewOrderInputs(userID, pair, orderType, side, price, quantity); err != nil {
		zap.S().Errorw("failed to create new order", "error", err)
		return nil
	}
	return &Order{
		UserID:    userID,
		Pair:      pair,
		Type:      orderType,
		Side:      side,
		Price:     price,
		Quantity:  quantity,
		Filled:    decimal.Zero,
		Status:    StatusOpen,
		Timestamp: time.Now(),
	}
}

func (o *Order) Remaining() decimal.Decimal {
	return o.Quantity.Sub(o.Filled)
}

func (o *Order) IsFilled() bool {
	return o.Remaining().IsZero()
}

func (o *Order) BeforeCreate(tx *gorm.DB) error {
	if o.ID == "" {
		o.ID = uuid.NewString()
	}
	return nil
}
