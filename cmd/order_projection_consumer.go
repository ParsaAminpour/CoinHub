package cmd

import (
	"coinhub/internal"
	"coinhub/internal/adapter/messaging/kafka"
	adapterkafka "coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/engine"
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

func RunOrderProjectionConsumer(configs *configs.Configuration) *cobra.Command {
	var pair string
	var groupID string
	var enableDLQ bool
	cmd := &cobra.Command{
		Use:   "order-projection-consumer",
		Short: "Run order projection Kafka consumer",
		Long:  "Run order projection Kafka consumer",
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
			// NOTE : this consumer includes all topics except Order Submittion event
			selectedTopics := adapterkafka.CoinhubAllOrderTopicsByEventTypes([]adapterkafka.EventType{
				adapterkafka.EventOrderFilled,
				adapterkafka.EventOrderPartial,
				adapterkafka.EventOrderStatus,
				adapterkafka.EventOrderCanceled,
				adapterkafka.EventOrderExpired,
				adapterkafka.EventTradeExecuted,
			}, "")

			zap.S().Infow("projection consumer subscribing", "topic", selectedTopics, "group_id", groupID)
			consumerClient, err := kgo.NewClient(
				kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)),
				kgo.ConsumeTopics(selectedTopics...),
				kgo.ConsumerGroup(groupID),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				zap.S().Fatalw("failed to create projection consumer client", "error", err)
			}
			defer consumerClient.Close()

			deduper, ok := app.OrderRepository.(interface {
				MarkEventProcessed(ctx context.Context, consumerName string, eventID string) (bool, error)
			})
			if !ok {
				zap.S().Fatal("order repository does not support idempotent event handling")
			}

			handler := &order_event_usecases.ProjectionHandler{
				OrderRepository: app.OrderRepository,
				Deduper:         deduper,
				ConsumerName:    groupID,
			}
			runner := kafkaconsumer.NewRunner(
				consumerClient,
				func(handlerCtx context.Context, event any, record *kgo.Record) error {
					orderEvent := event.(kafka.OrderStatusEvent)
					if orderEvent.EventType == adapterkafka.EventOrderExpired {
						return handler.HandleOrderExpiredEvent(handlerCtx, orderEvent, record)
					}
					if err := engine.ValidateOrderStatusEvent(orderEvent); err != nil {
						return err
					}
					return handler.UpdateOrderStatus(handlerCtx, orderEvent, record)
				},
				groupID,
				3,
				2*time.Second,
			)
			if enableDLQ {
				dlqProducerClient, dlqErr := kgo.NewClient(
					kgo.SeedBrokers(fmt.Sprintf("%s:%s", configs.MessageBroker.MessageStreamerHost, configs.MessageBroker.MessageStreamerPort)),
				)
				if dlqErr != nil {
					zap.S().Fatalw("failed to create projection dlq producer client", "error", dlqErr)
				}
				defer dlqProducerClient.Close()
				runner = runner.WithDLQ(dlqProducerClient, kafka.OrderProjectionConsumerDLQTopic)
				zap.S().Infow("DLQ enabled for projection consumer", "dlq_topic", "order-projection-consumer.dlq")
			}

			closeSignal := make(chan os.Signal, 1)
			signal.Notify(closeSignal, syscall.SIGTERM, syscall.SIGINT, os.Interrupt)
			defer signal.Stop(closeSignal)

			go func() {
				sig := <-closeSignal
				zap.S().Infow("projection consumer shutting down", "signal", sig.String())
				cancel()
			}()

			if err := runner.Run(ctx); err != nil {
				zap.S().Errorw("projection consumer stopped with error", "error", err)
			}
			app.Shutdown()
			zap.S().Debug("shutdown complete")
		},
	}
	cmd.Flags().StringVar(&pair, "pair", "BTC.USDT", "trading pair topic suffix")
	cmd.Flags().StringVar(&groupID, "group-id", kafka.OrderPrjectionConsumerGroupID, "kafka consumer group id")
	cmd.Flags().BoolVar(&enableDLQ, "enable-dlq", false, "enable dead-letter queue publishing to order-projection-consumer.dlq")
	return cmd
}
