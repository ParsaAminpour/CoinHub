package engine

import (
	"coinhub/internal/adapter/messaging/kafka"
	"strings"

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
	Run(ctx context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.EngineEventProducer, assetRepository *repositories.AssetRepository, orderRepository *repositories.OrderRepository, tradeRepository *repositories.TradeRepository, configs configs.Configuration) error
	orderMainConsumer(ctx context.Context, kafkaProducer *kafka.EngineEventProducer, orderRepository *repositories.OrderRepository, tradeRepository *repositories.TradeRepository, configs configs.Configuration, workerID string) error
	SubmitOrder(eventProducer *kafka.EngineEventProducer, order Order) error
	orderHandlerWorker(ctx context.Context, eventProducer *kafka.EngineEventProducer, pair string, ch chan *Order)
	TradeChConsumer(ctx context.Context) error
	Close()
	// OrderDispatcher(ctx context.Context, kafkaClient *kafka.OrderEventProducer, order *Order) error
}

type OrderRouter struct {
	workers map[string]chan *Order // BTC-USDT --map--> [ channel ] <--reads-- appropriate Goroutine
}

func (r *OrderRouter) addOrder(pair string, order *Order) {
	ch, ok := r.workers[pair]
	if !ok {
		zap.S().Errorw("no worker channel for pair, order dropped", "pair", pair)
		return
	}
	ch <- order
}

type MatchEngine struct {
	Orderbooks map[string]*Orderbook // one Orderbook per trading pair, e.g. "BTC-USDT" -> *Orderbook
	TradeChan  chan Trade            // Remove it till we use another process for running engine.

	OrderRouter        *OrderRouter
	OrderEventProducer *kafka.EngineEventProducer
	OrderEventConsumer *kafka.OrderEventConsumer
}

type SupportedPairLight struct {
	ID     string  `json:"id"`
	Symbol *string `json:"symbol"` // symbol should be in BTC-USDT format
}

func NewSupportedPairLight(id string, symbol string) *SupportedPairLight {
	supportedAssetLight := &SupportedPairLight{
		ID:     id,
		Symbol: &symbol,
	}
	if err := supportedAssetLight.fixSupportedPairLight(); err != nil {
		return nil
	}
	return supportedAssetLight
}

func (a *SupportedPairLight) fixSupportedPairLight() error {
	if !strings.Contains(*a.Symbol, "-") {
		if !strings.Contains(*a.Symbol, "/") || len(strings.Split(*a.Symbol, "/")) != 2 {
			return fmt.Errorf("the asset symbol format is incorrect %s", *a.Symbol)
		}
		formattedSymbol := fmt.Sprintf("%s-%s", strings.Split(*a.Symbol, "/")[0], strings.Split(*a.Symbol, "/")[1])
		a.Symbol = &formattedSymbol
	}
	return nil
}

// TODO : handle the errors too.
func NewMatchEngine(ctx context.Context, configs configs.Configuration, availableAssets []SupportedPairLight) Engine {
	orderEventProduced, _ := initializeEventProducer(ctx, configs)
	orderRouter, _ := initializeOrderRouter(ctx, availableAssets)
	orderbooks := initializeOrderbooks(availableAssets)
	return &MatchEngine{
		Orderbooks:         orderbooks,
		TradeChan:          make(chan Trade, TradeChanBufferSize),
		OrderRouter:        orderRouter,
		OrderEventProducer: orderEventProduced,
	}
}

func initializeOrderbooks(availableAssets []SupportedPairLight) map[string]*Orderbook {
	orderbooks := make(map[string]*Orderbook, len(availableAssets))
	for _, asset := range availableAssets {
		if asset.Symbol == nil {
			continue
		}
		orderbooks[*asset.Symbol] = &Orderbook{
			Pair: *asset.Symbol,
		}
	}
	return orderbooks
}

func (me *MatchEngine) Close() {
	if me.OrderEventProducer != nil {
		me.OrderEventProducer.Close()
	}
	if me.OrderEventConsumer != nil {
		me.OrderEventConsumer.Close()
	}
}

func initializeEventProducer(ctx context.Context, configs configs.Configuration) (*kafka.EngineEventProducer, error) {
	producerClient, err := kgo.NewClient(kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)))
	if err != nil {
		return nil, err
	}
	orderEventProducer := kafka.NewEngineEventProducer(ctx, producerClient)
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
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)
	if err != nil {
		zap.S().Fatalw("failed to create projection consumer client", "error", err)
		return nil, err
	}

	zap.S().Infow("Initializing Order Submission Event Consumer", "selectedTopics", selectedTopics)
	orderEventConsumer := kafka.NewOrderEventConsumer(ctx, consumerClient)
	return orderEventConsumer, nil
}

func initializeOrderSubmittionEventDLQ(ctx context.Context, configs configs.Configuration) (*kgo.Client, error) {
	dlqProducerCleient, dlqErr := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)),
	)
	if dlqErr != nil {
		zap.S().Errorw("failed to create DLQ producer client", "error", dlqErr)
	}

	// TODO : defer DLQ producer
	return dlqProducerCleient, nil
}

func initializeOrderRouter(ctx context.Context, availableAssets []SupportedPairLight) (*OrderRouter, error) {
	for _, asset := range availableAssets {
		if err := asset.fixSupportedPairLight(); err != nil {
			return nil, nil
		}
	}
	orderRouter := &OrderRouter{workers: make(map[string]chan *Order)}
	for _, pair := range availableAssets {
		ch := make(chan *Order, OrderChanBufferSize)
		orderRouter.workers[*pair.Symbol] = ch
	}
	return orderRouter, nil
}

// called by the HTTP handler, return 202 as accepted. another goroutine will process it asyncrounsly
// pushing the order into orderCh
func (me *MatchEngine) SubmitOrder(eventProducer *kafka.EngineEventProducer, incomingOrder Order) error {
	zap.S().Infow("Submitting order to match engine", "orderID", incomingOrder.ID, "userID", incomingOrder.UserID, "pair", incomingOrder.Pair, "type", incomingOrder.Type, "side", incomingOrder.Side, "price", incomingOrder.Price, "quantity", incomingOrder.Quantity)

	rawOrderEventForIncomingOrder := kafka.NewOrderEvent(
		incomingOrder.ID, incomingOrder.UserID, incomingOrder.Pair, kafka.OrderType(incomingOrder.Type), kafka.StatusOpen, kafka.EventOrderSubmitted,
		kafka.OrderSide(incomingOrder.Side), incomingOrder.Price, incomingOrder.Quantity, incomingOrder.Filled, incomingOrder.Remaining(),
	)

	if err := eventProducer.PublishOrderEvent(rawOrderEventForIncomingOrder); err != nil {
		zap.S().Errorw("Failed to publish order event", "error", err, "orderID", incomingOrder.ID)
		return fmt.Errorf("failed to publish order event: %w", err)
	}
	return nil
}

func (me *MatchEngine) orderTypeRouter(eventProducer *kafka.EngineEventProducer, order Order) error {
	ob, ok := me.Orderbooks[order.Pair]
	if !ok {
		zap.S().Errorw("no orderbook found for pair, order dropped", "pair", order.Pair, "orderID", order.ID)
		return fmt.Errorf("no orderbook for pair %s", order.Pair)
	}
	var err error
	switch order.Type {
	case OrderTypeLimit:
		_, err = ob.MatchLimit(eventProducer, order)
	case OrderTypeMarket:
		_, err = ob.MatchMarket(eventProducer, order)
	case OrderTypeCancel:
		_, err = ob.Cancel(order)
	}
	return err
}

// run this concurrently for each pair
func (me *MatchEngine) orderHandlerWorker(ctx context.Context, eventProducer *kafka.EngineEventProducer, pair string, ch chan *Order) {
	for order := range ch {
		zap.S().Infow("orderConsumerWorker received order",
			"channel", fmt.Sprintf("%p", ch),
			"orderID", order.ID,
			"userID", order.UserID,
			"pair", order.Pair,
			"type", order.Type,
			"side", order.Side,
			"price", order.Price,
			"quantity", order.Quantity,
		)
		// call the appropriate matching algorithm (synchronously)
		if err := me.orderTypeRouter(eventProducer, *order); err != nil {
			zap.S().Error("Failed to process order", err)
		}

		// the order will process and if there was any results, it will issue an FILLED event to the Broker.
		// the consumers will use it to update the DB and notify the user via the websocket.
	}
}

func (me *MatchEngine) routeOrderEventFanOut(eventProducer *kafka.EngineEventProducer, event adapterkafka.OrderStatusEvent) error {
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
	me.OrderRouter.addOrder(event.Pair, order)
	return nil
}

// act like a demultiplexer, Fan-out pattern, reads from original stream `me.OrderChan`
// NOTE : this function will run concurrently for each system available pair.
// NOTE : the number of workers is based on the number of available pairs that we support.
// @note this function get all orders comes from the main stream, then it route the incoming order to its assocaited engine-orderbook.
// TODO : run the consumer in better approach.
// Act as the main stream consumer. gives the result to the appropriate channel to handle that.
// It listens to the ORDER_SUBMITTED and TRADE_EXECUTED events and route the appropriate order to the appropriate channel.
func (me *MatchEngine) orderMainConsumer(ctx context.Context, kafkaProducer *kafka.EngineEventProducer, orderRepository *repositories.OrderRepository, tradeRepository *repositories.TradeRepository, configs configs.Configuration, workerID string) error {
	deduper, ok := (*orderRepository).(interface {
		MarkEventProcessed(ctx context.Context, consumerName string, eventID string) (bool, error)
	})
	if !ok {
		zap.S().Fatal("order repository does not support idempotent event handling")
	}
	handler := &order_event_usecases.ProjectionHandler{
		OrderRepository: *orderRepository,
		TradeRepository: *tradeRepository,
		Deduper:         deduper,
		ConsumerName:    kafka.OrderSubmittedConsumerGroupID,
	}
	runner := kafkaconsumer.NewRunner(
		me.OrderEventConsumer.GetConsumer(),
		func(handlerCtx context.Context, event any, record *kgo.Record) error {
			if err := order_event_usecases.ValidateStatusEvent(event.(adapterkafka.OrderStatusEvent)); err != nil {
				return err
			}
			zap.S().Info("Consuming order status event calls Handle for pair:", event.(adapterkafka.OrderStatusEvent).Pair)

			switch event.(type) {
			case adapterkafka.OrderStatusEvent:
				if err := handler.HandleIncmingOrder(handlerCtx, event.(adapterkafka.OrderStatusEvent), record); err != nil {
					return err
				}
				if err := me.routeOrderEventFanOut(kafkaProducer, event.(adapterkafka.OrderStatusEvent)); err != nil {
					return err
				}
			case adapterkafka.TradeStatusEvent:
				if err := handler.HandleTradeExecutedEvent(handlerCtx, event.(adapterkafka.TradeStatusEvent), record); err != nil {
					return err
				}
			}
			return nil
		},
		kafka.OrderSubmittedConsumerGroupID,
		3,
		2*time.Second,
	)

	submittionEventDLQ, err := initializeOrderSubmittionEventDLQ(ctx, configs)
	if err != nil {
		return err
	}
	defer submittionEventDLQ.Close()

	runner = runner.WithDLQ(submittionEventDLQ, kafka.OrderSubmittedConsumerDLQTopic)
	if err := runner.Run(ctx); err != nil {
		zap.S().Fatalw("projection consumer stopped with error", "error", err)
	}
	return nil
}

// fire and forget with quit channel, Already defined. One goroutine per pair, lives forever, clean shutdown via quit.
func (me *MatchEngine) Run(ctx /*backgroundCtx*/ context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.EngineEventProducer, assetRepo *repositories.AssetRepository, orderRepository *repositories.OrderRepository, tradeRepository *repositories.TradeRepository, configs configs.Configuration) error {
	orderEventConsumer, err := initializeOrderSubmittionEventConsumer(ctx, configs)
	if err != nil {
		return fmt.Errorf("failed to initialize order submission consumer: %w", err)
	}
	me.OrderEventConsumer = orderEventConsumer

	// one worker goroutine per pair — must start before orderMainConsumer begins routing into channels
	for pair, ch := range me.OrderRouter.workers {
		wg.Add(1)
		go func(pair string, ch chan *Order) {
			defer wg.Done()
			me.orderHandlerWorker(ctx, kafkaProducer, pair, ch)
		}(pair, ch)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		zap.S().Infow("orderMainConsumer goroutine started")
		if err := me.orderMainConsumer(ctx, kafkaProducer, orderRepository, tradeRepository, configs, "mock-worker-id"); err != nil {
			zap.S().Errorw("orderChConsumer error", "error", err)
		}
		zap.S().Infow("orderMainConsumer goroutine finished")
	}()

	go func() {
		<-ctx.Done()
		zap.S().Info("the context terminated")
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

func SetupMatchEngine(ctx context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.EngineEventProducer, assetRepository repositories.AssetRepository, orderRepository repositories.OrderRepository, tradeRepository repositories.TradeRepository, matchEngine *MatchEngine, configs configs.Configuration) error {
	zap.S().Info("Setting up match engine...")
	if err := matchEngine.Run(ctx, wg, kafkaProducer, &assetRepository, &orderRepository, &tradeRepository, configs); err != nil {
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
