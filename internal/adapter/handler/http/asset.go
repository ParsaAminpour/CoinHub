package http

import (
	"coinhub/internal/adapter/handler/http/helper"
	"coinhub/internal/adapter/handler/http/schema"
	"coinhub/internal/adapter/messaging/kafka"
	"coinhub/internal/domain/entities"
	"coinhub/internal/domain/repositories"
	"errors"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SystemHandler struct {
	TradingPairRepository repositories.TradingPairRepository
	AssetRepository       repositories.AssetRepository
	KafkaTopicManager     *kafka.TopicManager
}

func NewSystemHandler(
	tradingPairRepository repositories.TradingPairRepository,
	assetRepository repositories.AssetRepository,
	kafkaTopicManager *kafka.TopicManager,
) HttpAPIHandler {
	return &SystemHandler{
		TradingPairRepository: tradingPairRepository,
		AssetRepository:       assetRepository,
		KafkaTopicManager:     kafkaTopicManager,
	}
}

// CreateAssetAdminOperationHandler godoc
// @Summary      Create a new asset
// @Description  Admin operation to register a new EVM asset in the exchange
// @Tags         system
// @Accept       json
// @Produce      json
// @Param        request  body      schema.CreateAssetRequest   true  "Asset creation data"
// @Success      201      {object}  schema.CreateAssetResponse  "Asset created"
// @Failure      400      {object}  helper.ErrorResponse        "Invalid request body"
// @Failure      409      {object}  helper.ErrorResponse        "Asset address already exists"
// @Failure      500      {object}  helper.ErrorResponse        "Internal server error"
// @Router       /v1/system/operation/asset/add [post]
func CreateAssetAdminOperationHandler(c *gin.Context, handlerCtx *HttpAPIHandler) error {
	h, ok := (*handlerCtx).(*SystemHandler)
	if !ok {
		return errors.New("invalid handler context for CreateAssetAdminOperationHandler")
	}

	var req schema.CreateAssetRequest
	rh := helper.NewResponseHelper()
	if err := c.ShouldBindJSON(&req); err != nil {
		rh.InvalidRequestBody(c)
		return err
	}

	asset := entities.Asset{
		Name:                &req.Name,
		Symbol:              &req.Symbol,
		AssetAddress:        &req.AssetAddress,
		Network:             req.Network,
		NetworkAvailability: req.NetworkAvailability,
		MaxSize:             req.MaxSize,
		Status:              entities.AssetStatusActive,
	}

	if err := h.AssetRepository.Create(c, &asset); err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) {
			rh.Conflict(c, "asset with this address already exists")
			return err
		}
		rh.InternalServerError(c)
		return err
	}

	// add partition to the kafka topics with the new asset symbol
	newPairCount, _ := h.TradingPairRepository.GetActivePairsCount(c)
	if err := h.KafkaTopicManager.OnNewPair(c, newPairCount); err != nil {
		// Critical Resolver should resolve this issue
		zap.S().Errorw("failed to update Kafka topic partitions after new asset creation", "error", err, "newPairCount", newPairCount, "asset", asset)
	}

	c.JSON(201, schema.CreateAssetResponse{
		ID:                  asset.ID,
		Name:                *asset.Name,
		Symbol:              *asset.Symbol,
		AssetAddress:        *asset.AssetAddress,
		Network:             asset.Network,
		NetworkAvailability: asset.NetworkAvailability,
		MaxSize:             asset.MaxSize,
		Status:              asset.Status,
	})
	return nil
}
