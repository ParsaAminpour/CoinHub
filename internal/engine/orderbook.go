package engine

import (
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/infrastructure/metrics"
	"context"
	"sync"

	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

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
// MatchLimit attempts to match an incoming limit order against the existing order book levels.
// It processes the order according to price-time priority, supporting both buy and sell sides,
// and ensures no self-trading occurs. The function emits appropriate kafka order events via
// the provided OrderEventProducer to communicate order status changes. If a match is found,
// the trades are sent through a channel for settlement. Returns ErrOrderbookEmpty if there are
// no suitable opposing orders to match.
//
// Parameters:
//   - eventProducer: The kafka EventPublisher for broadcasting order and trade events.
//   - incomingOrder: The new incoming limit order to be matched against the current orderbook.
//
// Returns:
//   - A receive-only channel of Trade objects (matched trades if any).
//   - An error if no match is possible or an internal error occurs.
func (ob *Orderbook) MatchLimit(eventProducer kafka.EventPublisher, incomingOrder Order) error {
	if incomingOrder.Side == SideBuy {
		if ob.Asks.Levels.Len() == 0 {
			return ErrOrderbookEmpty
		}
		// NOTE : the only thing that is going to change is the filled, remainingQty, status and the eventType sections
		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		// the buyer doesn't want to go upper than incomingOrder.Price, and the best seller doesn't want to go lower than its price
		bestPriceLevel, exist := ob.Asks.BestPriceLevel()
		if !exist {
			return ErrNoBestOrderInOrderbook
		}
		if bestPriceLevel.PriceLevel.LessThanOrEqual(incomingOrder.Price) {
			// the incoming order matched and the settle accounts are going to start...
			incomingFullyFilled := false
			for _, bestOrder := range bestPriceLevel.Orders { // oldest order has time priority
				if bestOrder.UserID == incomingOrder.UserID { // prevent self-order and money-washing
					continue
				}
				fillQty := decimal.Min(bestOrder.Remaining(), incomingOrder.Remaining())
				rawOrderEventForBestOrder := kafka.NewOrderEvent(
					bestOrder.ID, bestOrder.UserID, bestOrder.Pair, kafka.OrderType(bestOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
					kafka.OrderSide(bestOrder.Side), bestOrder.Price, bestOrder.Quantity, bestOrder.Filled, bestOrder.Remaining(),
				)
				rawTradeEvent := kafka.NewTradeStatusEvent(
					bestOrder.UserID, incomingOrder.UserID, bestOrder.ID, incomingOrder.ID, incomingOrder.Pair, incomingOrder.Price, fillQty, false, false, bestOrder.Remaining(), incomingOrder.Remaining(),
				)

				if !fillQty.IsZero() {
					// Evaluate fill conditions before updating the filled amounts.
					restingFilled := fillQty.GreaterThanOrEqual(bestOrder.Remaining())
					incomingFilled := fillQty.GreaterThanOrEqual(incomingOrder.Remaining())

					if restingFilled {
						ob.Asks.PopFront()
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)
						rawTradeEvent.MakerFilled = true
					} else {
						bestOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)
					}

					if incomingFilled {
						incomingFullyFilled = true
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
						rawTradeEvent.TakerFilled = true
					} else {
						incomingOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
					}

					bestOrder.Filled = bestOrder.Filled.Add(fillQty)
					incomingOrder.Filled = incomingOrder.Filled.Add(fillQty)

					if err := eventProducer.PublishOrderEventBatch([]kafka.OrderEvent{rawOrderEventForBestOrder, rawOrderEventForIncomingOrder}); err != nil {
						zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", bestOrder.ID, "user_id", bestOrder.UserID, "pair", bestOrder.Pair)
					}
					if rawTradeEvent.MakerFilled || rawTradeEvent.TakerFilled {
						if err := eventProducer.PublishTradeStatusEvent(rawTradeEvent); err != nil {
							zap.S().Errorw("failed to publish trade event", "error", err, "maker_order_id", bestOrder.ID, "taker_order_id", incomingOrder.ID, "pair", incomingOrder.Pair)
						}
						metrics.TradesExecutedTotal.WithLabelValues(incomingOrder.Pair).Inc()
						metrics.OrdersMatchedTotal.WithLabelValues(incomingOrder.Pair).Inc()
					}
					if incomingFullyFilled {
						break
					}
				} else {
					zap.S().Infow("No matching order found for incoming order",
						"order_id", incomingOrder.ID,
						"price", incomingOrder.Price.String(),
						"remaining_qty", incomingOrder.Remaining().String(),
					)
				}
			}
			// Rest the remaining portion of the incoming order on the bid side if not fully filled.
			if !incomingFullyFilled && incomingOrder.Status == StatusPartial {
				ob.Bids.Add(&incomingOrder)
			}
		}

	} else {
		if ob.Bids.Levels.Len() == 0 {
			return ErrOrderbookEmpty
		}
		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		bestPriceLevel, exist := ob.Bids.BestPriceLevel()
		if !exist {
			return ErrNoBestOrderInOrderbook
		}
		if bestPriceLevel.PriceLevel.GreaterThanOrEqual(incomingOrder.Price) {
			incomingFullyFilled := false
			for _, bestOrder := range bestPriceLevel.Orders {
				if bestOrder.UserID == incomingOrder.UserID {
					continue
				}

				fillQty := decimal.Min(bestOrder.Remaining(), incomingOrder.Remaining())
				rawOrderEventForBestOrder := kafka.NewOrderEvent(
					bestOrder.ID, bestOrder.UserID, bestOrder.Pair, kafka.OrderType(bestOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
					kafka.OrderSide(bestOrder.Side), bestOrder.Price, bestOrder.Quantity, bestOrder.Filled, bestOrder.Remaining(),
				)
				rawTradeEvent := kafka.NewTradeStatusEvent(
					bestOrder.UserID, incomingOrder.UserID, bestOrder.ID, incomingOrder.ID, incomingOrder.Pair, incomingOrder.Price, fillQty, false, false, bestOrder.Remaining(), incomingOrder.Remaining(),
				)

				if !fillQty.IsZero() {
					// Evaluate fill conditions before updating the filled amounts.
					restingFilled := fillQty.GreaterThanOrEqual(bestOrder.Remaining())
					incomingFilled := fillQty.GreaterThanOrEqual(incomingOrder.Remaining())

					if restingFilled {
						ob.Bids.PopFront()
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)
						rawTradeEvent.MakerFilled = true
					} else {
						bestOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForBestOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForBestOrder.UpdateOrderFilled(fillQty)
					}

					if incomingFilled {
						incomingFullyFilled = true
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
						rawTradeEvent.TakerFilled = true
					} else {
						incomingOrder.ChangeStatusTo(StatusPartial)
						rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
						rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
					}

					if err := eventProducer.PublishOrderEventBatch([]kafka.OrderEvent{rawOrderEventForBestOrder, rawOrderEventForIncomingOrder}); err != nil {
						zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", bestOrder.ID, "user_id", bestOrder.UserID, "pair", bestOrder.Pair)
					}
					if rawTradeEvent.MakerFilled || rawTradeEvent.TakerFilled {
						if err := eventProducer.PublishTradeStatusEvent(rawTradeEvent); err != nil {
							zap.S().Errorw("failed to publish trade status event", "error", err, "maker_order_id", bestOrder.ID, "taker_order_id", incomingOrder.ID, "pair", incomingOrder.Pair)
						}
						metrics.TradesExecutedTotal.WithLabelValues(incomingOrder.Pair).Inc()
						metrics.OrdersMatchedTotal.WithLabelValues(incomingOrder.Pair).Inc()
					}

					bestOrder.Filled = bestOrder.Filled.Add(fillQty)
					incomingOrder.Filled = incomingOrder.Filled.Add(fillQty)

					if incomingFullyFilled {
						break
					}
				}
			}
			// Rest the remaining portion of the incoming order on the ask side if not fully filled.
			if !incomingFullyFilled && incomingOrder.Status == StatusPartial {
				ob.Asks.Add(&incomingOrder)
			}
		} else {
			// No match found for the incoming order
			rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderCanceled, kafka.StatusCancelled)
			if err := eventProducer.PublishOrderEvent(rawOrderEventForIncomingOrder); err != nil {
				zap.S().Errorw("failed to publish order status change event", "error", err, "order_id", incomingOrder.ID, "user_id", incomingOrder.UserID, "pair", incomingOrder.Pair)
			}
		}
	}

	return nil
}

// the price of incomingOrder is going to be the market price, not choosen by the ow
// MatchMarket attempts to match an incoming market order with available orders
// on the opposite side of the orderbook. For buy orders, it matches with best available asks;
// for sell orders, it matches with best available bids.
// It fills both the incoming and resting orders up to the minimum of their respective remaining quantities,
// and marks their status accordingly (partial, filled, cancelled).
// The function also emits relevant order events through Kafka, using the provided kafkaClient.
// Returns a channel of Trade objects (or nil if none), and an error if the market cannot be matched.
func (ob *Orderbook) MatchMarket(eventProducer kafka.EventPublisher, incomingOrder Order) error {
	if incomingOrder.Side == SideBuy {
		if ob.Asks.Levels.Len() == 0 {
			return ErrOrderbookEmpty
		}

		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		// Accumulates one event per consumed resting order; published as a single batch.
		restingOrderEvents := make([]kafka.OrderEvent, 0)
		tradeEvents := make([]kafka.TradeStatusEvent, 0)
		incomingFullyFilled := false
		cancelled := false

		ob.Asks.Levels.Ascend(func(priceLevel *PriceLevel) bool {
			if priceLevel.PriceLevel.GreaterThan(incomingOrder.Price) {
				// All remaining levels are too expensive — cancel the unfilled remainder.
				rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderCanceled, kafka.StatusCancelled)
				cancelled = true
				return false
			}

			// Index-based inner loop so we can safely remove filled orders mid-iteration.
			for idx := 0; idx < len(priceLevel.Orders); {
				restingOrder := priceLevel.Orders[idx]
				if restingOrder.ID == incomingOrder.ID {
					idx++
					return true
				}

				makerRemainingBefore := restingOrder.Remaining()
				takerRemainingBefore := incomingOrder.Remaining()
				fillQty := decimal.Min(makerRemainingBefore, takerRemainingBefore)
				if fillQty.IsZero() {
					idx++
					return true
				}

				rawOrderEventForRestingOrder := kafka.NewOrderEvent(
					restingOrder.ID, restingOrder.UserID, restingOrder.Pair, kafka.OrderType(restingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
					kafka.OrderSide(restingOrder.Side), restingOrder.Price, restingOrder.Quantity, restingOrder.Filled, restingOrder.Remaining(),
				)

				// Evaluate conditions before applying fills.
				restingFilled := fillQty.GreaterThanOrEqual(makerRemainingBefore)
				incomingFilled := fillQty.GreaterThanOrEqual(takerRemainingBefore)

				makerRemainingAfter := makerRemainingBefore.Sub(fillQty)
				takerRemainingAfter := takerRemainingBefore.Sub(fillQty)

				tradeEvents = append(tradeEvents, kafka.NewTradeStatusEvent(
					restingOrder.UserID,
					incomingOrder.UserID,
					restingOrder.ID,
					incomingOrder.ID,
					incomingOrder.Pair,
					restingOrder.Price, // market trades at maker/resting price
					fillQty,
					restingFilled,
					incomingFilled,
					makerRemainingAfter,
					takerRemainingAfter,
				))

				if restingFilled {
					priceLevel.RemoveFilledOrderInPriceLevel(incomingOrder.Side, idx)
					// idx stays the same — element at idx is now the next order.
					rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
					rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
				} else {
					restingOrder.ChangeStatusTo(StatusPartial)
					rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
					rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
					idx++
				}
				restingOrderEvents = append(restingOrderEvents, rawOrderEventForRestingOrder)

				if incomingFilled {
					incomingFullyFilled = true
					rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
					rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
				} else {
					incomingOrder.ChangeStatusTo(StatusPartial)
					rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
					rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
				}

				restingOrder.Filled = restingOrder.Filled.Add(fillQty)
				incomingOrder.Filled = incomingOrder.Filled.Add(fillQty)

				if incomingFullyFilled {
					return false
				}
			}
			return true
		})

		// Publish the accumulated batch (cancel, full fill, or partial-then-exhausted).
		if incomingFullyFilled || cancelled || len(restingOrderEvents) > 0 {
			allEvents := append(restingOrderEvents, rawOrderEventForIncomingOrder)
			if err := eventProducer.PublishOrderEventBatch(allEvents); err != nil {
				zap.S().Errorw("Failed to publish order event batch", "error", err)
			}
			if len(tradeEvents) > 0 {
				if err := eventProducer.PublishTradeStatusEventBatch(tradeEvents); err != nil {
					zap.S().Errorw("Failed to publish trade event batch", "error", err)
				}
				metrics.TradesExecutedTotal.WithLabelValues(incomingOrder.Pair).Add(float64(len(tradeEvents)))
			}
			if incomingFullyFilled {
				metrics.OrdersMatchedTotal.WithLabelValues(incomingOrder.Pair).Inc()
			}
		}

		// Remove price levels that were fully drained.
		ob.Asks.RemoveEmptyLevel()

	} else {
		if ob.Bids.Levels.Len() == 0 {
			return ErrOrderbookEmpty
		}

		rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
			incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
			kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
		)

		restingOrderEvents := make([]kafka.OrderEvent, 0)
		tradeEvents := make([]kafka.TradeStatusEvent, 0)
		incomingFullyFilled := false
		cancelled := false

		ob.Bids.Levels.Ascend(func(priceLevel *PriceLevel) bool {
			if priceLevel.PriceLevel.LessThan(incomingOrder.Price) {
				// All remaining bid levels are below the seller's minimum — cancel.
				rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderCanceled, kafka.StatusCancelled)
				cancelled = true
				return false
			}

			for idx := 0; idx < len(priceLevel.Orders); {
				restingOrder := priceLevel.Orders[idx]
				if restingOrder.UserID == incomingOrder.UserID {
					idx++
					return true // prevent self-order and money-washing
				}

				makerRemainingBefore := restingOrder.Remaining()
				takerRemainingBefore := incomingOrder.Remaining()
				fillQty := decimal.Min(makerRemainingBefore, takerRemainingBefore)
				if fillQty.IsZero() {
					idx++
					continue
				}

				rawOrderEventForRestingOrder := kafka.NewOrderEvent(
					restingOrder.ID, restingOrder.UserID, restingOrder.Pair, kafka.OrderType(restingOrder.Type), kafka.StatusPartial, kafka.EventOrderPartial,
					kafka.OrderSide(restingOrder.Side), restingOrder.Price, restingOrder.Quantity, restingOrder.Filled, restingOrder.Remaining(),
				)

				// Evaluate conditions before applying fills.
				restingFilled := fillQty.GreaterThanOrEqual(makerRemainingBefore)
				incomingFilled := fillQty.GreaterThanOrEqual(takerRemainingBefore)

				makerRemainingAfter := makerRemainingBefore.Sub(fillQty)
				takerRemainingAfter := takerRemainingBefore.Sub(fillQty)

				tradeEvents = append(tradeEvents, kafka.NewTradeStatusEvent(
					restingOrder.UserID,
					incomingOrder.UserID,
					restingOrder.ID,
					incomingOrder.ID,
					incomingOrder.Pair,
					restingOrder.Price, // market trades at maker/resting price
					fillQty,
					restingFilled,
					incomingFilled,
					makerRemainingAfter,
					takerRemainingAfter,
				))

				if restingFilled {
					priceLevel.RemoveFilledOrderInPriceLevel(incomingOrder.Side, idx)
					rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
					rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
				} else {
					restingOrder.ChangeStatusTo(StatusPartial)
					rawOrderEventForRestingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
					rawOrderEventForRestingOrder.UpdateOrderFilled(fillQty)
					idx++
				}
				restingOrderEvents = append(restingOrderEvents, rawOrderEventForRestingOrder)

				if incomingFilled {
					incomingFullyFilled = true
					rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderFilled, kafka.StatusFilled)
					rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
				} else {
					incomingOrder.ChangeStatusTo(StatusPartial)
					rawOrderEventForIncomingOrder.ChangeStatusEvent(kafka.EventOrderPartial, kafka.StatusPartial)
					rawOrderEventForIncomingOrder.UpdateOrderFilled(fillQty)
				}

				restingOrder.Filled = restingOrder.Filled.Add(fillQty)
				incomingOrder.Filled = incomingOrder.Filled.Add(fillQty)

				if incomingFullyFilled {
					return false
				}
			}
			return true
		})

		if incomingFullyFilled || cancelled || len(restingOrderEvents) > 0 {
			allEvents := append(restingOrderEvents, rawOrderEventForIncomingOrder)
			if err := eventProducer.PublishOrderEventBatch(allEvents); err != nil {
				zap.S().Errorw("Failed to publish order event batch", "error", err)
			}
			if len(tradeEvents) > 0 {
				if err := eventProducer.PublishTradeStatusEventBatch(tradeEvents); err != nil {
					zap.S().Errorw("Failed to publish trade event batch", "error", err)
				}
				metrics.TradesExecutedTotal.WithLabelValues(incomingOrder.Pair).Add(float64(len(tradeEvents)))
			}
			if incomingFullyFilled {
				metrics.OrdersMatchedTotal.WithLabelValues(incomingOrder.Pair).Inc()
			}
		}

		ob.Bids.RemoveEmptyLevel()
	}

	return nil
}

func (ob *Orderbook) Cancel(ctx context.Context, orderRepository repositories.OrderRepository, incomingOrder Order) error {
	zap.S().Infow("cancel order trigerred and consumed", "order", incomingOrder)
	var captured bool
	if incomingOrder.Side == SideBuy {
		pl, exist := ob.Bids.GetLevel(incomingOrder.Price)
		if exist {
			if err := pl.RemoveOrderInPriceLevelBasedOnOrderID(incomingOrder.ID); err != nil {
				zap.S().Errorw("failed to remove order in price level", "error", err, "orderID", incomingOrder.ID, "price", incomingOrder.Price)
				return err
			}
			captured = true
		} else {
			captured = false
		}
		ob.Bids.RemoveEmptyLevel()

	} else {
		pl, exist := ob.Asks.GetLevel(incomingOrder.Price)
		if exist {
			if err := pl.RemoveOrderInPriceLevelBasedOnOrderID(incomingOrder.ID); err != nil {
				zap.S().Errorw("failed to remove order in price level", "error", err, "orderID", incomingOrder.ID, "price", incomingOrder.Price)
				return err
			}
			captured = true
		} else {
			captured = false
		}
		ob.Asks.RemoveEmptyLevel()
	}

	if !captured {
		// TODO : Implement a Critical Resolver to manage this situation, add this to the asynq
		return ErrOrderNotFoundInOrderbook
	}
	if err := orderRepository.UpdateOrderStatus(ctx, incomingOrder.ID, entities.StatusCancelled, incomingOrder.Filled); err != nil {
		return err
	}
	metrics.OrdersCancelledTotal.WithLabelValues(incomingOrder.Pair).Inc()
	return nil
}
