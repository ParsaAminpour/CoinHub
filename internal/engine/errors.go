package engine

import "errors"

var (
	// Orderbook errors
	ErrOrderbookEmpty           = errors.New("orderbook is empty")
	ErrOrderNotValid            = errors.New("order is not valid")
	ErrOrderNotFoundInOrderbook = errors.New("order not found in orderbook")
	ErrNoOrderbookForPair       = errors.New("no orderbook for pair")
	ErrNoBestOrderInOrderbook   = errors.New("best order not found in orderbook")

	// Price level errors
	ErrOrderNotFoundInPriceLevel = errors.New("order not found in price level")

	// Event publishing errors
	ErrPublishOrderEventFailed  = errors.New("failed to publish order event")
	ErrPublishCancelEventFailed = errors.New("failed to publish cancel order event")
	ErrInitOrderConsumerFailed  = errors.New("failed to initialize order submission consumer")

	// Symbol/pair format errors
	ErrInvalidSymbolFormat = errors.New("invalid asset symbol format")
)
