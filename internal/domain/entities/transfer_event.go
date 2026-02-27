package entities

import (
	"math/big"
	"time"

	"github.com/google/uuid"
)

type TransferEvent struct {
	ID          uint      `gorm:"primaryKey;autoIncrement"`
	TxHash      string    `gorm:"uniqueIndex;not null"`
	BlockNumber uint64    `gorm:"not null"`
	From        string    `gorm:"not null"`
	To          string    `gorm:"not null"`
	Amount      string    `gorm:"not null"` // store as string, big.Int doesn't play well with gorm
	AssetID     uuid.UUID `gorm:"type:uuid;index;not null"`
	Asset       Asset     `gorm:"foreignKey:AssetID"`

	ReceivedAt time.Time `gorm:"autoCreateTime"`
	CreatedAt  time.Time
}

func NewTransferEvent(
	txHash string,
	blockNumber uint64,
	// contractAddress string,
	assetID uuid.UUID,
	from string,
	to string,
	amount string,
	tokenSymbol string,
	receivedAt time.Time,
) *TransferEvent {
	return &TransferEvent{
		TxHash:      txHash,
		BlockNumber: blockNumber,
		AssetID:     assetID,
		From:        from,
		To:          to,
		Amount:      amount,
		ReceivedAt:  receivedAt,
	}
}

// helper to get Amount as big.Int
func (t *TransferEvent) GetAmount() *big.Int {
	amount := new(big.Int)
	amount.SetString(t.Amount, 10)
	return amount
}
