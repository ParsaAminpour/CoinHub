package entities

import (
	"math/big"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type TransactionType string

const (
	Legacy  TransactionType = "legacy"
	EIP2930 TransactionType = "eip2930"
	EIP1559 TransactionType = "eip1559"
	EIP712  TransactionType = "eip712"
)

type EVMTransaction struct {
	gorm.Model
	// Foreign key to User (one user can have many EVM transactions)
	UserID uuid.UUID `gorm:"type:uuid;index;not null"`
	User   User      `gorm:"foreignKey:UserID"`

	Hash        string          `gorm:"type:varchar(66);uniqueIndex;not null"` // typical EVM tx hash length
	Chain_id    int             `gorm:"not null"`
	Nonce       big.Int         `gorm:"not null"`
	FromAddress string          `gorm:"type:varchar(42);not null"` // EVM address length with '0x'
	ToAddress   *string         `gorm:"type:varchar(42)"`          // nullable
	Value       int             `gorm:"not null"`
	CallData    string          `gorm:"type:text"`
	Type        TransactionType `gorm:"type:varchar(16);not null"`
}

func NewEVMTransaction(
	hash string,
	chainID int,
	nonce big.Int,
	fromAddress string,
	toAddress *string,
	value int,
	callData string,
	txType TransactionType,
) EVMTransaction {
	return EVMTransaction{
		Hash:        hash,
		Chain_id:    chainID,
		Nonce:       nonce,
		FromAddress: fromAddress,
		ToAddress:   toAddress,
		Value:       value,
		CallData:    callData,
		Type:        txType,
	}
}
