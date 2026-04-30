package http

import (
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/repositories"
	"errors"

	"github.com/gin-gonic/gin"
)

type SystemHandler struct {
	TradingPairRepository repositories.TradingPairRepository
	KafkaTopicManager     *kafka.TopicManager
}

func NewSystemHandler(
	tradingPairRepository repositories.TradingPairRepository,
	kafkaTopicManager *kafka.TopicManager,
) HttpAPIHandler {
	return &SystemHandler{
		TradingPairRepository: tradingPairRepository,
		KafkaTopicManager:     kafkaTopicManager,
	}
}

func CreateAssetAdminOperationHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*SystemHandler)
	if !ok {
		return errors.New("invalid handler context for CreateAssetAdminOperationHandler")
	}
	// TODO: implement asset creation logic
	newPairCount, _ := h.TradingPairRepository.GetActivePairsCount(c)
	h.KafkaTopicManager.OnNewPair(c, newPairCount)
	return nil
}
