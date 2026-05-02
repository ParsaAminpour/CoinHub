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
type OrderBehavior string

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

	TIF OrderBehavior = "TIF" // time-in-force 			> for limit orders
	GTC OrderBehavior = "GTC" // good-til-canceled 		> no special behavior for order, no expiry
	IOC OrderBehavior = "IOC" // immediate-or-cancel 	> for market orders
	ALO OrderBehavior = "ALO" // add-liquidity-only 	> post only
)

// DefaultRestingOrderLifetime is the max time a resting limit order may stay in the order book
// when the client does not send an explicit expire_at.
const DefaultRestingOrderLifetime = 24 * time.Hour
const MaxRestingDays = 30 // the maximum lifetime for an order to resting is 30days

// RestingOrderExpiresAt returns the absolute expiry for a resting order: optional user time
// or placement time plus DefaultRestingOrderLifetime.
func RestingOrderExpiresAt(userExpire *time.Time, placement time.Time) time.Time {
	t := placement.UTC()
	if userExpire != nil && !userExpire.IsZero() {
		return userExpire.UTC()
	}
	return t.Add(DefaultRestingOrderLifetime)
}

type Order struct {
	ID        string          `gorm:"primaryKey;default:gen_random_uuid()"` // Unique order identifier
	UserID    string          `gorm:"not null;index"`                       // User that placed the order
	User      User            `gorm:"foreignKey:UserID"`
	Pair      string          `gorm:"type:varchar(20);not null;index"`          // Trading pair, e.g., "BTC/USDT"
	Type      OrderType       `gorm:"type:varchar(10);not null"`                // Order type: limit, market, cancel
	Side      OrderSide       `gorm:"type:varchar(10);not null"`                // Order side: buy or sell
	Behavior  OrderBehavior   `gorm:"type:varchar(10);not null;default:'GTC'"`  // Time-in-force / execution behavior (default GTC)
	Price     decimal.Decimal `gorm:"type:numeric;default:0"`                   // Order price (0 for market orders)
	Quantity  decimal.Decimal `gorm:"type:numeric;not null"`                    // Order quantity
	Filled    decimal.Decimal `gorm:"type:numeric;default:0"`                   // Quantity filled so far
	Status    OrderStatus     `gorm:"type:varchar(15);not null;default:'open'"` // Order status
	ExpiresAt time.Time       `gorm:"not null;default:'2001-09-11 00:00:00'"`   // When the resting order expires in the book (default: 11 September 2001), I'm jocking btw
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
	expireAt *time.Time,
) *Order {
	if err := validateNewOrderInputs(userID, pair, orderType, side, price, quantity); err != nil {
		zap.S().Errorw("failed to create new order", "error", err)
		return nil
	}
	now := time.Now()
	return &Order{
		UserID:    userID,
		Pair:      pair,
		Type:      orderType,
		Side:      side,
		Behavior:  GTC,
		Price:     price,
		Quantity:  quantity,
		Filled:    decimal.Zero,
		Status:    StatusOpen,
		ExpiresAt: RestingOrderExpiresAt(expireAt, now),
		Timestamp: now,
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
	if o.Behavior == "" {
		o.Behavior = GTC
	}
	if o.ExpiresAt.IsZero() {
		basis := o.Timestamp
		if basis.IsZero() {
			basis = time.Now()
		}
		o.ExpiresAt = RestingOrderExpiresAt(nil, basis)
	}
	return nil
}
