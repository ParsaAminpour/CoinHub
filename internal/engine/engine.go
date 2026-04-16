package engine

import (
	"coinhub/internal/adapter/messaging/kafka"

	adapterkafka "coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/usecases/order_event_usecases"

	kafkaconsumer "coinhub/internal/infrastructure/kafka/consumer"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// this section will run a goroutine running a price-time priority match engine associated to each supported pairs

const (
	OrderChanBufferSize int64 = 10_000
	TradeChanBufferSize int64 = 10_000 // TODO : do we need buffered channel for the trade?
)

// [HTTP Handlers]  →  orderCh  →  [Engine.Run()]  →  tradeCh  →  [Trade Consumer]
//
//	(many)              buffer       (1 per pair)       buffer        (1+)
type Engine interface {
	Run(ctx context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.OrderEventProducer, assetRepository *repositories.AssetRepository, orderRepository *repositories.OrderRepository) error
	SubmitOrder(eventProducer *kafka.OrderEventProducer, order Order) error
	orderChConsumer(ctx context.Context, kafkaProducer *kafka.OrderEventProducer, orderRepository *repositories.OrderRepository, workerID string) error
	TradeChConsumer(ctx context.Context) error
	// OrderDispatcher(ctx context.Context, kafkaClient *kafka.OrderEventProducer, order *Order) error
}

type MatchEngine struct {
	Orderbook Orderbook
	OrderChan chan Order // Remove it till we use another process for running engine.
	TradeChan chan Trade // Remove it till we use another process for running engine.

	OrderEventProducer *kafka.OrderEventProducer
	OrderEventConsumer *kafka.OrderEventConsumer
}

// TODO : handle the errors too.
func NewMatchEngine(ctx context.Context, configs configs.Configuration) Engine {
	orderEventProduced, _ := initializeOrderSubmittionEventProducer(ctx, configs)
	orderEventConsumer, _ := initializeOrderSubmittionEventConsumer(ctx, configs)
	return &MatchEngine{
		OrderChan:          make(chan Order, OrderChanBufferSize),
		TradeChan:          make(chan Trade, TradeChanBufferSize),
		OrderEventProducer: orderEventProduced,
		OrderEventConsumer: orderEventConsumer,
	}
}

func initializeOrderSubmittionEventProducer(ctx context.Context, configs configs.Configuration) (*kafka.OrderEventProducer, error) {
	producerClient, err := kgo.NewClient(kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)))
	if err != nil {
		return nil, err
	}
	orderEventProducer := kafka.NewOrderEventProducer(ctx, producerClient)
	zap.S().Infow("Initializing Order Submission Event Producer",
		"host", configs.MessageBroker.MessageStreamerHost,
		"port", configs.MessageBroker.MessageStreamerPort,
	)
	return orderEventProducer, nil
}

func initializeOrderSubmittionEventConsumer(ctx context.Context, configs configs.Configuration) (*kafka.OrderEventConsumer, error) {
	selectedTopics := kafka.CoinHubOrderSubmittedTopic("")
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)),
		kgo.ConsumeTopics(selectedTopics),
		kgo.ConsumerGroup(kafka.OrderSubmittedConsumerGroupID),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		zap.S().Fatalw("failed to create projection consumer client", "error", err)
		return nil, err
	}

	zap.S().Info("Initializing Order Submission Event Consumer", "selectedTopics", selectedTopics)
	orderEventConsumer := kafka.NewOrderEventConsumer(ctx, consumerClient)
	return orderEventConsumer, nil
}

// called by the HTTP handler, return 202 as accepted. another goroutine will process it asyncrounsly
// pushing the order into orderCh
func (me *MatchEngine) SubmitOrder(eventProducer *kafka.OrderEventProducer, incomingOrder Order) error {
	zap.S().Infow("Submitting order to match engine", "orderID", incomingOrder.ID, "userID", incomingOrder.UserID, "pair", incomingOrder.Pair, "type", incomingOrder.Type, "side", incomingOrder.Side, "price", incomingOrder.Price, "quantity", incomingOrder.Quantity)

	rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
		incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusOpen, kafka.EventOrderSubmitted,
		kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
	)

	if err := eventProducer.PublishOrderEvent(rawOrderEventForIncomingOrder); err != nil {
		zap.S().Errorw("Failed to publish order event", "error", err, "orderID", incomingOrder.ID)
		return fmt.Errorf("failed to publish order event: %w", err)
	}
	zap.S().Infow("Pushing order to order channel", "orderID", incomingOrder.ID)
	return nil
}

func (me *MatchEngine) orderTypeRouter(eventProducer *kafka.OrderEventProducer, order Order) error {
	var err error
	// var tradeCh <-chan Trade
	switch order.Type {
	case OrderTypeLimit:
		_, err = me.Orderbook.MatchLimit(eventProducer, order)
	case OrderTypeMarket:
		_, err = me.Orderbook.MatchMarket(eventProducer, order)
	case OrderTypeCancel:
		_, err = me.Orderbook.Cancel(order)
	}
	// handle the tradeCh
	return err
}

func (me *MatchEngine) routeOrderEventFanOut(eventProducer *kafka.OrderEventProducer, event adapterkafka.OrderStatusEvent) error {
	// TODOD : implement it...
	zap.S().Infow("orderRouter received event",
		"event_id", event.EventID,
		"order_id", event.ID,
		"user_id", event.UserID,
		"pair", event.Pair,
		"order_type", event.Type,
		"side", event.Side,
		"price", event.Price,
		"quantity", event.Quantity,
		"filled", event.Filled,
		"status", event.Status,
		"remaining_qty", event.RemainingQty,
		"event_type", event.EventType,
		"event_version", event.Version,
		"event_occured_at", event.OccuredAt,
	)

	// TODO : routes the order to its appropriate goroutine (map[pair:string]chan)
	order := NewOrder(
		event.UserID,
		event.Pair,
		OrderType(event.Type),
		OrderSide(event.Side),
		event.Price,
		event.Quantity,
	)
	me.Orderbook.MatchLimit(eventProducer, *order)
	return nil
}

// act like a demultiplexer, Fan-out pattern, reads from original stream `me.OrderChan`
// NOTE : this function will run concurrently for each system available pair.
// NOTE : the number of workers is based on the number of available pairs that we support.
// @note this function get all orders comes from the main stream, then it route the incoming order to its assocaited engine-orderbook.
// TODO : run the consumer in better approach.
func (me *MatchEngine) orderChConsumer(ctx context.Context, kafkaProducer *kafka.OrderEventProducer, orderRepository *repositories.OrderRepository, workerID string) error {
	zap.S().Infow("orderChConsumer started", "workerID", workerID)

	deduper, ok := (*orderRepository).(interface {
		MarkEventProcessed(ctx context.Context, consumerName string, eventID string) (bool, error)
	})
	if !ok {
		zap.S().Fatal("order repository does not support idempotent event handling")
	}
	handler := &order_event_usecases.ProjectionHandler{
		OrderRepository: *orderRepository,
		Deduper:         deduper,
		ConsumerName:    kafka.OrderSubmittedConsumerGroupID,
	}
	runner := kafkaconsumer.NewRunner(
		me.OrderEventConsumer.GetConsumer(),
		func(handlerCtx context.Context, event adapterkafka.OrderStatusEvent, record *kgo.Record) error {
			if err := order_event_usecases.ValidateStatusEvent(event); err != nil {
				return err
			}
			if err := handler.Handle(handlerCtx, event, record); err != nil {
				return err
			}
			return me.routeOrderEventFanOut(kafkaProducer, event)
		},
		kafka.OrderSubmittedConsumerGroupID,
		3,
		2*time.Second,
	)
	if err := runner.Run(ctx); err != nil {
		zap.S().Fatalw("projection consumer stopped with error", "error", err)
	}
	return nil
}

// func (me *MatchEngine) OrderDispatcher(ctx context.Context, kafkaClient *kafka.OrderEventProducer, order *Order) error {
// 	// runs for each pair
// 	// it gets the order from the orderCh
// 	// calls the Match or Cancel based on the type of order dequeued from the channel.
// 	for incomingOrder := range me.OrderChan {
// 		switch incomingOrder.Type {
// 		case OrderTypeMarket:
// 			if _, err := me.Orderbook.MatchMarket(kafkaClient, incomingOrder); err != nil {
// 				continue
// 			}
// 		case OrderTypeLimit:
// 			if _, err := me.Orderbook.MatchLimit(nil, incomingOrder); err != nil {
// 				continue
// 			}
// 		case OrderTypeCancel:
// 			if _, err := me.Orderbook.Cancel(incomingOrder); err != nil {
// 				continue
// 			}
// 		}
// 	}
// 	return nil
// }

// fire and forget with quit channel, Already defined. One goroutine per pair, lives forever, clean shutdown via quit.
func (me *MatchEngine) Run(ctx /*backgroundCtx*/ context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.OrderEventProducer, assetRepo *repositories.AssetRepository, orderRepository *repositories.OrderRepository) error {
	numberOfGoroutines, err := (*assetRepo).GetAvailableAssetsCount(ctx)
	if err != nil {
		return err
	}
	zap.S().Infow("Starting match engine goroutines", "numPairs", numberOfGoroutines)

	wg.Add(1)
	go func() {
		if err := me.orderChConsumer(ctx, kafkaProducer, orderRepository, "mock-worker-id"); err != nil {
			zap.S().Errorw("orderChConsumer error", "error", err)
		}
		zap.S().Infow("Order Consumer goroutine started")
	}()

	go func() {
		select {
		case <-ctx.Done():
			zap.S().Info("the context terminated")
			// Optionally: close channels or perform cleanup here if needed
		}
	}()

	wg.Wait()
	return nil
}

func (me *MatchEngine) TradeChConsumer(ctx context.Context) error {
	for _ = range me.TradeChan {
		// implement here...
	}
	return nil
}

func SetupMatchEngine(ctx context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.OrderEventProducer, assetRepository repositories.AssetRepository, orderRepository repositories.OrderRepository, matchEngine *MatchEngine) error {
	zap.S().Info("Setting up match engine...")
	if err := matchEngine.Run(ctx, wg, kafkaProducer, &assetRepository, &orderRepository); err != nil {
		return err
	}
	return nil
}

func (me *MatchEngine) CloseOrderbookGracefuly(ctx context.Context) error {
	// graceful shutdown the message queues.
	return nil
}

// should return the trades, the orders that get matched and become filled or partial
// wire the channels and orderbook matching algorithm with eachothers.
// func (me *MatchEngine) RunMatchEngineForPair(ctx context.Context) error {

// 	return nil
// }
