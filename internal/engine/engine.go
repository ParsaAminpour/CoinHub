package engine

import (
	"coinhub/internal/adapter/messaging/kafka"
	"container/heap"

	adapterkafka "coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/repositories"
	"coinhub/internal/infrastructure/configs"
	"coinhub/internal/infrastructure/metrics"
	"coinhub/internal/usecases/order_event_usecases"

	kafkaconsumer "coinhub/internal/infrastructure/kafka/consumer"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// NOTE: the google/btree is not thread-safe

const (
	OrderChanBufferSize int64 = 10_000
)

// [HTTP Handlers]  →  orderCh  →  [Engine.Run()]  →  tradeCh  →  [Trade Consumer]
//
//	(many)              buffer       (1 per pair)       buffer        (1+)
type Engine interface {
	Run(ctx context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.EngineEventProducer, assetRepository *repositories.AssetRepository, orderRepository *repositories.OrderRepository, tradeRepository *repositories.TradeRepository, configs configs.Configuration) error
	orderMainConsumer(ctx context.Context, kafkaProducer *kafka.EngineEventProducer, orderRepository *repositories.OrderRepository, tradeRepository *repositories.TradeRepository, configs configs.Configuration, workerID string) error
	SubmitOrder(eventProducer *kafka.EngineEventProducer, order Order) error
	orderHandlerWorker(ctx context.Context, eventProducer *kafka.EngineEventProducer, pair string, ch chan *Order)
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

	OrderRouter        *OrderRouter
	OrderEventProducer *kafka.EngineEventProducer
	OrderEventConsumer *kafka.OrderEventConsumer

	Reaper *BookReaper // synthetic order heap
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

func NewMatchEngine(ctx context.Context, configs configs.Configuration, availableAssets []SupportedPairLight) (Engine, error) {
	orderEventProduced, err := initializeEventProducer(ctx, configs)
	if err != nil {
		return nil, err
	}
	orderRouter, err := initializeOrderRouter(ctx, availableAssets)
	if err != nil {
		return nil, err
	}
	orderbooks := initializeOrderbooks(availableAssets)
	orderReaper := initializeReaper(ctx)
	return &MatchEngine{
		Orderbooks:         orderbooks,
		OrderRouter:        orderRouter,
		OrderEventProducer: orderEventProduced,
		Reaper:             &orderReaper,
	}, nil
}

func initializeOrderbooks(availableAssets []SupportedPairLight) map[string]*Orderbook {
	orderbooks := make(map[string]*Orderbook, len(availableAssets))
	for _, asset := range availableAssets {
		if asset.Symbol == nil {
			continue
		}
		orderbooks[*asset.Symbol] = &Orderbook{
			Pair: *asset.Symbol,
			Bids: NewSide(32, false),
			Asks: NewSide(32, true),
		}
	}
	return orderbooks
}

func initializeReaper(ctx context.Context) BookReaper {
	reaper := make(BookReaper, 0)
	heap.Init(&reaper)
	return reaper
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
	selectedTopics := []string{kafka.CoinHubOrderSubmittedTopic(""), kafka.CoinHubOrderCanceledTopic("")}
	consumerClient, err := kgo.NewClient(
		kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)),
		kgo.ConsumeTopics(selectedTopics...),
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
		kafka.OrderBehavior(incomingOrder.Behavior),
		incomingOrder.ExpiresAt,
	)

	if err := eventProducer.PublishOrderEvent(rawOrderEventForIncomingOrder); err != nil {
		zap.S().Errorw("Failed to publish order event", "error", err, "orderID", incomingOrder.ID)
		return fmt.Errorf("%w: %w", ErrPublishOrderEventFailed, err)
	}

	// add the order to the synthtic order heap (not orderbook)
	item := NewExpiryEntity(incomingOrder.ExpiresAt, incomingOrder.Pair, incomingOrder.ID, incomingOrder.Price, incomingOrder.Side, me.Reaper.Len())
	heap.Push(me.Reaper, item)

	metrics.OrdersSubmittedTotal.WithLabelValues(incomingOrder.Pair, string(incomingOrder.Type), string(incomingOrder.Side)).Inc()
	return nil
}

func (me *MatchEngine) SubmitCancelOrder(eventProducer *kafka.EngineEventProducer, incomingCancelOrder Order) error {
	zap.S().Infow("Submitting cancel order to match engine", "orderID", incomingCancelOrder.ID, "userID", incomingCancelOrder.UserID, "pair", incomingCancelOrder.Pair, "type", incomingCancelOrder.Type, "side", incomingCancelOrder.Side, "price", incomingCancelOrder.Price, "quantity", incomingCancelOrder.Quantity)
	rawCancelOrderEvent := kafka.NewOrderEvent(
		incomingCancelOrder.ID,
		incomingCancelOrder.UserID,
		incomingCancelOrder.Pair,
		kafka.OrderType(incomingCancelOrder.Type),
		kafka.StatusCancelled,
		kafka.EventOrderCanceled,
		kafka.OrderSide(incomingCancelOrder.Side),
		incomingCancelOrder.Price,
		incomingCancelOrder.Quantity,
		incomingCancelOrder.Filled,
		incomingCancelOrder.Remaining(),
		kafka.OrderBehavior(incomingCancelOrder.Behavior),
		incomingCancelOrder.ExpiresAt,
	)
	if err := eventProducer.PublishOrderEvent(rawCancelOrderEvent); err != nil {
		zap.S().Errorw("Failed to publish cancel order event", "error", err, "orderID", incomingCancelOrder.ID)
		return fmt.Errorf("%w: %w", ErrPublishCancelEventFailed, err)
	}
	return nil
}

func (me *MatchEngine) orderTypeRouter(eventProducer *kafka.EngineEventProducer, order Order) error {
	ob, ok := me.Orderbooks[order.Pair]
	if !ok {
		zap.S().Errorw("no orderbook found for pair, order dropped", "pair", order.Pair, "orderID", order.ID)
		return fmt.Errorf("%w: %s", ErrNoOrderbookForPair, order.Pair)
	}
	var err error
	switch order.Type {
	case OrderTypeLimit:
		err = ob.MatchLimit(eventProducer, order)
	case OrderTypeMarket:
		err = ob.MatchMarket(eventProducer, order)
	case OrderTypeCancel:
		err = ob.Cancel(context.Background(), nil, order)
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
	}
}

func (me *MatchEngine) routeOrderEventFanOut(eventProducer *kafka.EngineEventProducer, event adapterkafka.OrderStatusEvent) error {
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
		"expires_at", event.ExpiresAt,
		"event_type", event.EventType,
		"event_version", event.Version,
		"event_occured_at", event.OccuredAt,
	)

	var expPtr *time.Time
	if !event.ExpiresAt.IsZero() {
		exp := event.ExpiresAt.UTC()
		expPtr = &exp
	}
	order := NewOrder(
		event.UserID,
		event.Pair,
		OrderType(event.Type),
		OrderSide(event.Side),
		event.Price,
		event.Quantity,
		expPtr,
	)
	if event.Behavior != "" {
		order.Behavior = OrderBehavior(event.Behavior)
	}
	me.OrderRouter.addOrder(event.Pair, order)
	return nil
}

// Acts as the main event consumer for order-related events from Kafka.
// Runs as a single worker, not per pair. Responsible for receiving events from the stream and dispatching them to the correct per-pair order handler via the OrderRouter.
// Handles ORDER_SUBMITTED and TRADE_EXECUTED events, processing or routing each event as needed.
// This function ensures that incoming orders and trades are demultiplexed and handled by the corresponding pair's orderbook worker channel.
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
			zap.S().Info("Consuming order status event calls Handle for pair:", event.(adapterkafka.OrderStatusEvent).Pair)

			metrics.KafkaEventsConsumedTotal.WithLabelValues(record.Topic).Inc()
			switch event.(type) {
			case adapterkafka.OrderStatusEvent:
				if err := ValidateOrderStatusEvent(event.(adapterkafka.OrderStatusEvent)); err != nil {
					return err
				}
				if err := handler.HandleIncmingOrder(handlerCtx, event.(adapterkafka.OrderStatusEvent), record); err != nil {
					return err
				}
				if err := me.routeOrderEventFanOut(kafkaProducer, event.(adapterkafka.OrderStatusEvent)); err != nil {
					return err
				}
			case adapterkafka.TradeStatusEvent:
				if err := ValidateTradeStatusEvent(event.(adapterkafka.TradeStatusEvent)); err != nil {
					return err
				}
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
		return fmt.Errorf("%w: %w", ErrInitOrderConsumerFailed, err)
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		zap.S().Info("orderReaper goroutine started")
		if err := me.orderReaper(ctx, kafkaProducer); err != nil {
			zap.S().Errorw("orderReaper error", "error", err)
		}
		zap.S().Info("orderReaper goroutine finished")
	}()

	go func() {
		<-ctx.Done()
		zap.S().Info("the context terminated")
	}()

	wg.Wait()
	return nil
}

func (me *MatchEngine) orderReaper(ctx context.Context, kafkaProducer *kafka.EngineEventProducer) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// drain all expired entries at the top of the min-heap
			for me.Reaper.Len() > 0 {
				top := (*me.Reaper)[0]
				if !top.isExpired() {
					break // min-heap: nothing deeper is expired either
				}
				item := heap.Pop(me.Reaper).(*expiryEntity)
				// check the order existence in the orderbook
				if me.Orderbooks[item.pair].OrderExist(ctx, item.side, item.orderID) {
					removed := me.Orderbooks[item.pair].RemoveOrderByOrderID(ctx, item.side, item.orderID, item.price)
					if !removed {
						zap.S().Warnw("reaper: order not found for removal — may have been filled or cancelled",
							"order_id", item.orderID, "pair", item.pair, "side", item.side, "price", item.price, "expires_at", item.expiresAt,
						)
						metrics.ReaperRemovalFailedTotal.WithLabelValues(item.pair).Inc()
						continue
					}
					metrics.OrdersExpiredTotal.WithLabelValues(item.pair).Inc()
					// Create new expiry event (emit to Kafka OrderExpired topic)
					expiryEvent := kafka.NewOrderEvent(
						item.orderID,
						"", // userID unknown from expiryEntity, can be filled if tracked elsewhere
						item.pair,
						kafka.OrderType(OrderTypeLimit), // Assumed type; adjust if needed
						kafka.StatusExpired,             // Status for expired
						kafka.EventOrderExpired,         // Mark as order expired
						kafka.OrderSide(item.side),
						item.price,
						decimal.Zero,               // Quantity unknown here
						decimal.Zero,               // Filled unknown here
						decimal.Zero,               // Remaining unknown here
						kafka.OrderBehavior("GTC"), // Could not infer from expiryEntity
						item.expiresAt,
					)
					// other operations will handle by the associated consumer for EXPIRATION event
					if err := kafkaProducer.PublishOrderEvent(expiryEvent); err != nil {
						zap.S().Errorw("Failed to publish order expiry event", "error", err, "orderID", item.orderID)
					}
				}
			}
		}
	}
}

func SetupMatchEngine(ctx context.Context, wg *sync.WaitGroup, kafkaProducer *kafka.EngineEventProducer, assetRepository repositories.AssetRepository, orderRepository repositories.OrderRepository, tradeRepository repositories.TradeRepository, matchEngine *MatchEngine, configs configs.Configuration) error {
	zap.S().Info("Setting up match engine...")
	if err := matchEngine.Run(ctx, wg, kafkaProducer, &assetRepository, &orderRepository, &tradeRepository, configs); err != nil {
		return err
	}
	return nil
}

func (me *MatchEngine) CloseOrderbookGracefuly(ctx context.Context) error {
	// TODO : graceful shutdown the message queues.
	return nil
}

func (me *MatchEngine) Close() {
	if me.OrderEventProducer != nil {
		me.OrderEventProducer.Close()
	}
	if me.OrderEventConsumer != nil {
		me.OrderEventConsumer.Close()
	}
}
