package engine

import (
	"coinhub/internal/adapter/messaging/kafka"
	adapterkafka "coinhub/internal/adapter/messaging/kafka"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type EventType interface {
	adapterkafka.OrderStatusEvent | adapterkafka.TradeStatusEvent
}

func (a *SupportedPairLight) fixSupportedPairLight() error {
	if !strings.Contains(*a.Symbol, "-") {
		if !strings.Contains(*a.Symbol, "/") || len(strings.Split(*a.Symbol, "/")) != 2 {
			return fmt.Errorf("%w: %s", ErrInvalidSymbolFormat, *a.Symbol)
		}
		formattedSymbol := fmt.Sprintf("%s-%s", strings.Split(*a.Symbol, "/")[0], strings.Split(*a.Symbol, "/")[1])
		a.Symbol = &formattedSymbol
	}
	return nil
}

// removeEmptyLevels filters out price levels that have no remaining orders.
func removeEmptyLevels(levels []*PriceLevel) []*PriceLevel {
	result := levels[:0]
	for _, lvl := range levels {
		if len(lvl.Orders) > 0 {
			result = append(result, lvl)
		}
	}
	return result
}

func ValidateOrderStatusEvent(event kafka.OrderStatusEvent) error {
	if event.EventHeader.Version != "v1" {
		return fmt.Errorf("unsupported event version: %s", event.EventHeader.Version)
	}
	if event.ID == "" || event.UserID == "" || event.Pair == "" {
		err := errors.New("missing required event fields")
		zap.S().Errorw("missing required event fields",
			"error", err,
			"event_id", event.ID,
			"user_id", event.UserID,
			"pair", event.Pair,
			"event_header_version", event.EventHeader.Version,
		)
		return errors.New("missing required event fields")
	}
	return nil
}

func ValidateTradeStatusEvent(event kafka.TradeStatusEvent) error {
	if event.EventHeader.Version != "v1" {
		return fmt.Errorf("unsupported event version: %s", event.EventHeader.Version)
	}
	if event.MakerUserID == "" {
		return fmt.Errorf("missing maker user id")
	}
	if event.TakerUserID == "" {
		return fmt.Errorf("missing taker user id")
	}
	if event.MakerOrderID == "" {
		return fmt.Errorf("missing maker order id")
	}
	if event.TakerOrderID == "" {
		return fmt.Errorf("missing taker order id")
	}
	if event.Pair == "" {
		return fmt.Errorf("missing pair")
	}
	if !(strings.Contains(event.Pair, "-") || strings.Contains(event.Pair, "/")) {
		return fmt.Errorf("pair should be in format BASE-QUOTE or BASE/QUOTE")
	}
	if event.Price.IsNegative() {
		return fmt.Errorf("price should not be negative")
	}
	if event.Quantity.IsNegative() {
		return fmt.Errorf("quantity should not be negative")
	}
	return nil
}
