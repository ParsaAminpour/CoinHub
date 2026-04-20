package cmd

import (
	"coinhub/internal"
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/infrastructure/configs"
	kafkaconsumer "coinhub/internal/infrastructure/kafka/consumer"
	"coinhub/internal/usecases/order_event_usecases"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func RunNotificationConsumer(configs *configs.Configuration) *cobra.Command {
	var pair string
	var groupID string
	cmd := &cobra.Command{
		Use:   "notification-consumer",
		Short: "Run websocket notification Kafka consumer",
		Long:  "Run websocket notification Kafka consumer",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			app := internal.NewApplication(ctx, configs, internal.ApplicationOptions{
				CommandName:       cmd.Name(),
				SkipHDWallet:      true,
				SkipRedis:         true,
				SkipETHClient:     true,
				SkipWalletService: true,
				SkipAsynq:         true,
				SkipMail:          true,
				SkipCache:         true,
				SkipMatchEngine:   true,
				SkipMessageBroker: true,
			})

			topics := kafka.CoinhubAllCurrentOrderTopics()
			consumerClient, err := kgo.NewClient(
				kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)),
				kgo.ConsumeTopics(topics...),
				kgo.ConsumerGroup(groupID),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				zap.S().Fatalw("failed to create notification consumer client", "error", err)
			}
			defer consumerClient.Close()

			dlqProducerClient, err := kgo.NewClient(
				kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)),
			)
			if err != nil {
				zap.S().Fatalw("failed to create notification dlq producer client", "error", err)
			}
			defer dlqProducerClient.Close()

			deduper, ok := app.OrderRepository.(interface {
				MarkEventProcessed(ctx context.Context, consumerName string, eventID string) (bool, error)
			})
			if !ok {
				zap.S().Fatal("order repository does not support idempotent event handling")
			}

			handler := &order_event_usecases.NotificationHandler{
				Deduper:      deduper,
				ConsumerName: groupID,
			}
			runner := kafkaconsumer.NewRunner(
				consumerClient,
				func(handlerCtx context.Context, event any, record *kgo.Record) error {
					if err := order_event_usecases.ValidateStatusEvent(event.(kafka.OrderStatusEvent)); err != nil {
						return err
					}
					if err := handler.HandleNotificationForOrders(handlerCtx, event.(kafka.OrderStatusEvent), record, app.WebsocketNotificationServer); err != nil {
						return err
					}
					if err := handler.HandleNotificationForTrades(handlerCtx, event.(kafka.TradeStatusEvent), record, app.WebsocketNotificationServer); err != nil {
						return err
					}
					return nil
				},
				groupID,
				3,
				2*time.Second,
			).WithDLQ(dlqProducerClient, kafka.OrderProjectionConsumerDLQTopic)

			closeSignal := make(chan os.Signal, 1)
			signal.Notify(closeSignal, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
			defer signal.Stop(closeSignal)

			go func() {
				sig := <-closeSignal
				zap.S().Infow("notification consumer shutting down", "signal", sig.String())
				cancel()
			}()

			if err := runner.Run(ctx); err != nil {
				zap.S().Fatalw("notification consumer stopped with error", "error", err)
			}
		},
	}
	cmd.Flags().StringVar(&pair, "pair", "BTC.USDT", "trading pair topic suffix")
	cmd.Flags().StringVar(&groupID, "group-id", "order-notification-consumer-v1", "kafka consumer group id")
	return cmd
}
