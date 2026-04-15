package entities

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
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

func NewOrder(
	userID string,
	pair string,
	orderType OrderType,
	side OrderSide,
	price decimal.Decimal,
	quantity decimal.Decimal,
) *Order {
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
