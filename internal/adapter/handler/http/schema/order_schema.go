package schema

import (
	"reflect"
	"strings"
	"time"
	"unicode"

	"github.com/go-playground/validator/v10"
	"github.com/shopspring/decimal"
)

type PlaceOrderRequest struct {
	UserID      int64       `json:"user_id"       validate:"required"`
	ClientOrdID string      `json:"client_ord_id" validate:"required,max=36,client_ord_id"`
	Symbol      string      `json:"symbol"        validate:"required,symbol_format"` // e.g. BTC-USDT
	Side        Side        `json:"side"          validate:"required,oneof=BUY SELL"`
	OrderType   OrdType     `json:"ord_type"      validate:"required,oneof=LIMIT MARKET STOP_LIMIT IOC FOK POST_ONLY"`
	Price       string      `json:"price"         validate:"omitempty,decimal_gt0"`
	StopPrice   string      `json:"stop_price"    validate:"omitempty,decimal_gt0"`
	Qty         string      `json:"qty"           validate:"required,decimal_gt0"`
	TimeInForce TimeInForce `json:"time_in_force" validate:"omitempty,oneof=GTC GTD IOC FOK"`
	ExpireAt    *time.Time  `json:"expire_at"     validate:"omitempty,future_time"`
	Source      string      `json:"source"        validate:"omitempty,oneof=WEB MOBILE API"`
	RequestID   string      `json:"request_id"    validate:"omitempty,uuid4"`
}

func validateDecimalGT0(fl validator.FieldLevel) bool {
	raw := strings.TrimSpace(fl.Field().String())
	if raw == "" {
		return false
	}
	d, err := decimal.NewFromString(raw)
	if err != nil {
		return false
	}
	return d.IsPositive()
}

func validateClientOrdID(fl validator.FieldLevel) bool {
	for _, r := range fl.Field().String() {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func validateSymbolFormat(fl validator.FieldLevel) bool {
	parts := strings.Split(fl.Field().String(), "-")
	if len(parts) != 2 {
		return false
	}
	for _, part := range parts {
		if len(part) < 2 || len(part) > 10 {
			return false
		}
		for _, r := range part {
			if !unicode.IsUpper(r) {
				return false
			}
		}
	}
	return true
}

func validateFutureTime(fl validator.FieldLevel) bool {
	field := fl.Field()
	if field.Kind() == reflect.Ptr {
		if field.IsNil() {
			return true
		}
		field = field.Elem()
	}
	t, ok := field.Interface().(time.Time)
	if !ok {
		return false
	}
	return t.After(time.Now().UTC())
}

func validatePlaceOrderRequest(sl validator.StructLevel) {
	req, ok := sl.Current().Interface().(PlaceOrderRequest)
	if !ok {
		return
	}

	switch req.OrderType {
	case Limit, PostOnly:
		if strings.TrimSpace(req.Price) == "" {
			sl.ReportError(req.Price, "price", "Price", "price_required_for_limit", "")
		}
		if strings.TrimSpace(req.StopPrice) != "" {
			sl.ReportError(req.StopPrice, "stop_price", "StopPrice", "stop_price_not_allowed", "")
		}

	case Market:
		if strings.TrimSpace(req.Price) != "" {
			sl.ReportError(req.Price, "price", "Price", "price_not_allowed_for_market", "")
		}
		if strings.TrimSpace(req.StopPrice) != "" {
			sl.ReportError(req.StopPrice, "stop_price", "StopPrice", "stop_price_not_allowed", "")
		}

	case StopLimit:
		if strings.TrimSpace(req.Price) == "" {
			sl.ReportError(req.Price, "price", "Price", "price_required_for_stop_limit", "")
		}
		if strings.TrimSpace(req.StopPrice) == "" {
			sl.ReportError(req.StopPrice, "stop_price", "StopPrice", "stop_price_required", "")
		}
		price, err1 := decimal.NewFromString(req.Price)
		stop, err2 := decimal.NewFromString(req.StopPrice)
		if err1 == nil && err2 == nil {
			if req.Side == Buy && stop.LessThanOrEqual(price) {
				sl.ReportError(req.StopPrice, "stop_price", "StopPrice", "stop_price_must_exceed_price_for_buy", "")
			}
			if req.Side == Sell && stop.GreaterThanOrEqual(price) {
				sl.ReportError(req.StopPrice, "stop_price", "StopPrice", "stop_price_must_be_below_price_for_sell", "")
			}
		}

	case IOC, FOK:
		if strings.TrimSpace(req.Price) == "" {
			sl.ReportError(req.Price, "price", "Price", "price_required_for_ioc_fok", "")
		}
	}

	if req.TimeInForce == GTD {
		if req.ExpireAt == nil {
			sl.ReportError(req.ExpireAt, "expire_at", "ExpireAt", "expire_at_required_for_gtd", "")
		}
	}
	if req.TimeInForce != GTD && req.ExpireAt != nil {
		sl.ReportError(req.ExpireAt, "expire_at", "ExpireAt", "expire_at_only_valid_for_gtd", "")
	}
}

type OrderResponse struct {
	OrderID     int64       `json:"order_id"`
	ClientOrdID string      `json:"client_ord_id"`
	Symbol      string      `json:"symbol"`
	Side        Side        `json:"side"`
	OrdType     OrdType     `json:"ord_type"`
	TimeInForce TimeInForce `json:"time_in_force"`
	Price       string      `json:"price,omitempty"`
	StopPrice   string      `json:"stop_price,omitempty"`
	Qty         string      `json:"qty"`
	FilledQty   string      `json:"filled_qty"`
	AvgPrice    string      `json:"avg_price,omitempty"`
	Status      OrderStatus `json:"status"`
	CreatedAt   time.Time   `json:"created_at"`
}

// Supporting enums
type Side string
type OrdType string
type TimeInForce string
type OrderStatus string

const (
	StatusNew         OrderStatus = "NEW"          // accepted, not yet matched
	StatusPartialFill OrderStatus = "PARTIAL_FILL" // partially matched, resting
	StatusFilled      OrderStatus = "FILLED"       // fully matched
	StatusCancelled   OrderStatus = "CANCELLED"    // cancelled by user or system
	StatusRejected    OrderStatus = "REJECTED"     // failed validation post-acceptance
	StatusExpired     OrderStatus = "EXPIRED"      // GTD order past ExpireAt
)

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"

	Limit     OrdType = "LIMIT"
	Market    OrdType = "MARKET"
	StopLimit OrdType = "STOP_LIMIT"
	IOC       OrdType = "IOC"
	FOK       OrdType = "FOK"
	PostOnly  OrdType = "POST_ONLY"

	GTC     TimeInForce = "GTC" // Good Till Cancel
	GTD     TimeInForce = "GTD" // Good Till Date
	TIF_IOC TimeInForce = "IOC"
	TIF_FOK TimeInForce = "FOK"
)
