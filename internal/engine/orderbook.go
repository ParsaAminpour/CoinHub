package engine

import (
	"coinhub/internal/adapter/messaging/kafka"
	"errors"
	"sync"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

var (
	ErrOrderbookEmpty = errors.New("orderbook is empty")
	ErrOrderNotValid  = errors.New("order is not valid")
)

type PriceLevel struct {
	PriceLevel decimal.Decimal
	Orders     []*Order // TODO : using B-Tree instead of slices
}

func NewPriceLevel(orders []*Order, price decimal.Decimal) *PriceLevel {
	return &PriceLevel{
		PriceLevel: price,
		Orders:     orders,
	}
}

type Side struct {
	// The last order of the slice has the lowest price level
	Levels []*PriceLevel // could be bids or asks
}

func (s Side) Add(order Order, price decimal.Decimal) error {
	if len(s.Levels) == 0 {
		priceLevel := NewPriceLevel([]*Order{&order}, price)
		_ = append(s.Levels, priceLevel)
	}

	if order.Side == SideBuy {
		for idx, orderPl := range s.Levels { // s.Levels are descending start from idx:0
			nextPriceLevel := s.Levels[idx+1]
			if orderPl.PriceLevel.Equal(price) {
				_ = append(orderPl.Orders, &order) // oldest goes to the last

			} else if orderPl.PriceLevel.LessThan(price) && (nextPriceLevel == nil || nextPriceLevel.PriceLevel.GreaterThan(price)) {
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append(s.Levels[:idx+1], append([]*PriceLevel{priceLevel}, s.Levels[idx+1:]...)...)

			} else if orderPl.PriceLevel.LessThan(price) { // new best bid offer
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append([]*PriceLevel{priceLevel}, s.Levels...)
			}
		}
	} else {
		for idx, orderPl := range s.Levels { // the sequence goes ascending start from the idx:0
			nextPriceLevel := s.Levels[idx+1]
			if orderPl.PriceLevel.Equal(price) {
				_ = append(orderPl.Orders, &order) // oldest goes to the last

			} else if orderPl.PriceLevel.GreaterThan(price) && (nextPriceLevel == nil || nextPriceLevel.PriceLevel.LessThan(price)) {
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append(s.Levels[:idx+1], append([]*PriceLevel{priceLevel}, s.Levels[idx+1:]...)...)

			} else if orderPl.PriceLevel.LessThan(price) { // new best ask offer
				priceLevel := NewPriceLevel([]*Order{&order}, price)
				s.Levels = append([]*PriceLevel{priceLevel}, s.Levels...)
			}
		}
	}
	return nil
}

// the best price level for the bids is the first index, meaning the buyer wants to buy at highest price level.
// the best price level for the asks is the first index, meaning the seller wants to sell at lowest price level.
func (s *Side) BestPriceLevel() *PriceLevel {
	if len(s.Levels) == 0 {
		return nil
	}
	return s.Levels[0]
}

func (s *Side) RemovePriceLevel(price decimal.Decimal) error {
	// implement it..
	return nil
}

func (s *Side) PopFront() {
	if len(s.Levels) == 0 {
		return
	}
	best := s.Levels[0]
	best.Orders = best.Orders[1:]
	if len(best.Orders) == 0 {
		s.Levels = s.Levels[1:]
	}
}

func (s *PriceLevel) RemoveFilledOrderInPriceLevel(side OrderSide, idx int) {
	s.Orders = append(s.Orders[:idx], s.Orders[idx+1:]...)
}

// Asks → lowest price at index 0 (best ask = cheapest seller)
// Bids → highest price at index 0 (best bid = most generous buyer)
// index 0 id always the best price
type Orderbook struct {
	Pair string          // each orderbook is associated with a Pair
	Dust decimal.Decimal // each Pair has its own Dust amount
	Bids Side            // descending
	Asks Side            // ascending
	mu   sync.RWMutex
}

// NOTE : you can use any Matching algorithm you want based on the data structure you use for the orders
// TODO : Remove the logs after testing
// MatchLimit attempts to match an incoming limit order against the existing order book levels.
// It processes the order according to price-time priority, supporting both buy and sell sides,
// and ensures no self-trading occurs. The function emits appropriate kafka order events via
// the provided OrderEventProducer to communicate order status changes. If a match is found,
// the trades are sent through a channel for settlement. Returns ErrOrderbookEmpty if there are
// no suitable opposing orders to match.
//
// Parameters:
//   - eventProducer: The kafka OrderEventProducer for broadcasting order and trade events.
//   - incomingOrder: The new incoming limit order to be matched against the current orderbook.
//
// Returns:
//   - A receive-only channel of Trade objects (matched trades if any).
//   - An error if no match is possible or an internal error occurs.
func (ob *Orderbook) MatchLimit(eventProducer *kafka.OrderEventProducer, incomingOrder Order) (<-chan Trade, error) {
	if incomingOrder.Side == SideBuy {
		if len(ob.Asks.Levels) == 0 {
			return nil, ErrOrderbookEmpty
		}
		// NOTE : the only thing that is going to change is the filled, remainingQty, status and the eventType sections
		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		// the buyer doesn't want to go upper than incomingOrder.Price, and the best seller doesn't want to go lower than its price
		bestPriceLevel := ob.Asks.BestPriceLevel()
		if bestPriceLevel.PriceLevel.LessThanOrEqual(incomingOrder.Price) {
			// the incoming order matched and the settle accounts are going to start...
			for _, bestOrder := range bestPriceLevel.Orders { // the first firtsrone (oldest one that appended) has more time priority
				if bestOrder.UserID == incomingOrder.UserID { // prevent self-order and money-washing
					continue // go to the next best order
				}
				rawOrderEventForBestOrder := kafka.NewOrderEvent(
					bestOrder.ID, bestOrder.UserID, bestOrder.Pair, kafka.OrderType(bestOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
					kafka.OrderSide(bestOrder.Side), bestOrder.Price, bestOrder.Quantity, bestOrder.Filled, bestOrder.Remaining(),
				)
				fillQty := decimal.Min(bestOrder.Remaining(), incomingOrder.Remaining())

				if !fillQty.IsZero() {
					if bestOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) { // the order gets filled
						// remove that best order from the queue
						ob.Asks.PopFront()
						// notify that the order gets filled - Emit TradeEvent to the partitions
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)

					} else { // the order get filled partialy
						bestOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)
					}

					// changes related to the incoming order
					if incomingOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) {
						// don't keep the incoming order in orderbook
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
						// notify that the order gets filled
					} else { // stay in the orderbook
						incomingOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)

						if err := ob.Bids.Add(incomingOrder, incomingOrder.Price); err != nil {
							zap.S().Errorw("Failed to add incoming order to bids", "order_id", incomingOrder.ID, "price", incomingOrder.Price)
						}
					}
					bestOrder.Filled.Add(fillQty)
					incomingOrder.Filled.Add(fillQty)

				} else {
					zap.S().Infow("No matching order found for incoming order",
						"order_id", incomingOrder.ID,
						"price", incomingOrder.Price.String(),
						"remaining_qty", incomingOrder.Remaining().String(),
					)
				}

				// NOTE : Is it appropriate to publish the event related to the resting order first.
				if err := eventProducer.PublishOrderEventBatch([]kafka.OrderEvent{rawOrderEventForBestOrder, rawOrderEventForIncomingOrder}); err != nil {
					zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", bestOrder.ID, "user_id", bestOrder.UserID, "pair", bestOrder.Pair)
					// Q: how to handle this failure?
				}
			}
		}

	} else {
		if len(ob.Bids.Levels) == 0 {
			return nil, ErrOrderbookEmpty
		}
		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		bestPriceLevel := ob.Bids.BestPriceLevel()
		if bestPriceLevel.PriceLevel.GreaterThanOrEqual(incomingOrder.Price) {
			for _, bestOrder := range bestPriceLevel.Orders {
				if bestOrder.UserID == incomingOrder.UserID {
					continue
				}

				fillQty := decimal.Min(bestOrder.Price, incomingOrder.Price)
				rawOrderEventForBestOrder := kafka.NewOrderEvent(
					bestOrder.ID, bestOrder.UserID, bestOrder.Pair, kafka.OrderType(bestOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
					kafka.OrderSide(bestOrder.Side), bestOrder.Price, bestOrder.Quantity, bestOrder.Filled, bestOrder.Remaining(),
				)

				if !fillQty.IsZero() {
					if bestOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) {
						// remove that best order from the queue
						ob.Bids.PopFront()

						// notify that the order gets filled - Emit TradeEvent to the partitions
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)
					} else {
						bestOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)
					}

					if incomingOrder.Remaining().IsZero() || bestOrder.Remaining().LessThanOrEqual(ob.Dust) {
						// don't keep the incoming order in orderbook
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
						// notify that the order gets filled
					} else { // stay in the orderbook
						incomingOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
						if err := ob.Asks.Add(incomingOrder, incomingOrder.Price); err != nil {
							zap.S().Errorw("Failed to add incoming order to asks", "order_id", incomingOrder.ID, "price", incomingOrder.Price)
						}
					}
					// NOTE : Is it appropriate to publish the event related to the resting order first.
					if err := eventProducer.PublishOrderEventBatch([]kafka.OrderEvent{rawOrderEventForBestOrder, rawOrderEventForIncomingOrder}); err != nil {
						zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", bestOrder.ID, "user_id", bestOrder.UserID, "pair", bestOrder.Pair)
						// Q: how to handle this failure?
					}

					bestOrder.Filled.Add(fillQty)
					incomingOrder.Filled.Add(fillQty)
				}

			}
		} else {
			// No match found for the incoming order
			rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderCanceled, kafka.StatusCancelled)
			if err := eventProducer.PublishOrderEvent(rawOrderEventForIncomingOrder); err != nil {
				zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", incomingOrder.ID, "user_id", incomingOrder.UserID, "pair", incomingOrder.Pair)
			}
		}
	}

	return nil, nil // TODO : Return trade if there was any match
}

// the price of incomingOrder is going to be the market price, not choosen by the ow
// MatchMarket attempts to match an incoming market order with available orders
// on the opposite side of the orderbook. For buy orders, it matches with best available asks;
// for sell orders, it matches with best available bids.
// It fills both the incoming and resting orders up to the minimum of their respective remaining quantities,
// and marks their status accordingly (partial, filled, cancelled).
// The function also emits relevant order events through Kafka, using the provided kafkaClient.
// Returns a channel of Trade objects (or nil if none), and an error if the market cannot be matched.
func (ob *Orderbook) MatchMarket(eventProducer *kafka.OrderEventProducer, incomingOrder Order) (<-chan Trade, error) {
	if incomingOrder.Side == SideBuy {
		if len(ob.Asks.Levels) == 0 {
			return nil, ErrOrderbookEmpty
		}

		// NOTE : the only thing that is going to change is the filled, remainingQty, status and the eventType sections
		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		restingOrderEvents := make([]kafka.OrderEvent, 0) // in this scenario (market order) we have multiple best resting orders
		for _, priceLevel := range ob.Asks.Levels {
			if priceLevel.PriceLevel.LessThanOrEqual(incomingOrder.Price) {
				for idx, restingOrder := range priceLevel.Orders {
					if restingOrder.ID == incomingOrder.ID {
						continue
					}
					fillQty := decimal.Min(restingOrder.Remaining(), incomingOrder.Remaining())
					rawOrderEventForRestingOrder := kafka.NewOrderEvent(
						restingOrder.ID, restingOrder.UserID, restingOrder.Pair, kafka.OrderType(restingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
						kafka.OrderSide(restingOrder.Side), restingOrder.Price, restingOrder.Quantity, restingOrder.Filled, restingOrder.Remaining(),
					)

					if restingOrder.IsFilled() || restingOrder.Remaining().LessThanOrEqual(ob.Dust) {
						// remove that order from the queue and emit the filled event
						priceLevel.RemoveFilledOrderInPriceLevel(incomingOrder.Side, idx)
						rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
					} else {
						restingOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
					}
					restingOrderEvents = append(restingOrderEvents, rawOrderEventForRestingOrder)

					if incomingOrder.IsFilled() || incomingOrder.Remaining().LessThanOrEqual(ob.Dust) {
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)

					} else {
						incomingOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
						continue
					}

					if err := eventProducer.PublishOrderEventBatch(append(restingOrderEvents, rawOrderEventForIncomingOrder)); err != nil {
						// handle or log error if needed, currently just swallowing it
						zap.S().Errorw("Failed to publish order event batch", "error", err)
					}
					restingOrder.Filled.Add(fillQty)
					incomingOrder.Filled.Add(fillQty)

				}

			} else {
				// There are two scenario:
				// 1. the incoming market order is partially filled but there is no match in the other side, so the order should be revert! -> order get partial tag and the we ignore the remaining for the incoming order!
				// TODO : handle the partial filled - canceled situation.
				// 2. we don't have an appropriate order to match with the incoming market order! and order get cancelled tag.
				rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderCanceled, kafka.StatusCancelled)
				if err := eventProducer.PublishOrderEvent(rawOrderEventForIncomingOrder); err != nil {
					zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", incomingOrder.ID, "user_id", incomingOrder.UserID, "pair", incomingOrder.Pair)
				}
				break
			}
		}

	} else {
		if len(ob.Bids.Levels) == 0 {
			return nil, ErrOrderbookEmpty
		}

		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		restingOrderEvents := make([]kafka.OrderEvent, 0)
		for _, priceLevel := range ob.Bids.Levels {
			if priceLevel.PriceLevel.GreaterThanOrEqual(incomingOrder.Price) {
				for idx, restingOrder := range priceLevel.Orders {
					if restingOrder.UserID == incomingOrder.UserID {
						continue // prevent self-order and money-washing
					}
					rawOrderEventForRestingOrder := kafka.NewOrderEvent(
						restingOrder.ID, restingOrder.UserID, restingOrder.Pair, kafka.OrderType(restingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
						kafka.OrderSide(restingOrder.Side), restingOrder.Price, restingOrder.Quantity, restingOrder.Filled, restingOrder.Remaining(),
					)
					fillQty := decimal.Min(restingOrder.Remaining(), incomingOrder.Remaining())

					if !fillQty.IsZero() {
						if restingOrder.Remaining().IsZero() || restingOrder.Remaining().LessThanOrEqual(ob.Dust) {
							priceLevel.RemoveFilledOrderInPriceLevel(incomingOrder.Side, idx)
							rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
							rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
						} else {
							restingOrder.ChangeStatusTo(StatusPartial)
							rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
							rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
						}
						restingOrderEvents = append(restingOrderEvents, rawOrderEventForRestingOrder)

						if incomingOrder.Remaining().IsZero() || restingOrder.Remaining().LessThanOrEqual(ob.Dust) {
							rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
							rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
						} else {
							incomingOrder.ChangeStatusTo(StatusPartial)
							rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
							rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
							continue
						}

						if err := eventProducer.PublishOrderEventBatch(append(restingOrderEvents, rawOrderEventForIncomingOrder)); err != nil {
							zap.S().Errorw("Failed to publish order event batch", "error", err)
						}
						restingOrder.Filled.Add(fillQty)
						incomingOrder.Filled.Add(fillQty)
					}
				}
			} else {
				rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderCanceled, kafka.StatusCancelled)
				if err := eventProducer.PublishOrderEvent(rawOrderEventForIncomingOrder); err != nil {
					zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", incomingOrder.ID, "user_id", incomingOrder.UserID, "pair", incomingOrder.Pair)
				}
				break
			}
		}
	}

	return nil, nil // return trade chan if there was any match.
}

// TODO : implement it
func (ob *Orderbook) Cancel(incomingOrder Order) (<-chan Trade, error) {
	return nil, nil
}
