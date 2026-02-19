package entities

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type TransactionType string
type TransactionStatus string

const (
	Legacy  TransactionType = "legacy"
	EIP2930 TransactionType = "eip2930"
	EIP1559 TransactionType = "eip1559"
	EIP712  TransactionType = "eip712"
)

const (
	PendingStatus   TransactionStatus = "PENDING"
	ConfirmedStatus TransactionStatus = "CONFIRMED"
	FailedStatus    TransactionStatus = "FAILED"
	RevertedStatus  TransactionStatus = "REVERTED"
)

type EvmTransaction struct {
	gorm.Model
	// Foreign key to User (one user can have many EVM transactions)
	UserID uuid.UUID `gorm:"type:uuid;index;not null"`
	User   User      `gorm:"foreignKey:UserID"`

	Hash        *string           `gorm:"type:varchar(66);uniqueIndex"` // typical EVM tx hash length
	Chain_id    int               `gorm:"not null"`
	Nonce       int               `gorm:"not null"`
	FromAddress *string           `gorm:"type:varchar(42);not null"` // EVM address length with '0x'
	ToAddress   *string           `gorm:"type:varchar(42)"`          // nullable
	Value       int               `gorm:"not null"`
	CallData    *string           `gorm:"type:text"`
	Type        TransactionType   `gorm:"type:varchar(16);not null"`
	Status      TransactionStatus `gorm:"type:varchar(16);not null;default:'PENDING'"`
	Note        *string           `gorm:"type:text"` // Make Note an optional field
}

func NewEVMTransaction(
	userId uuid.UUID,
	hash *string,
	chainID int,
	nonce int,
	fromAddress string,
	toAddress *string,
	value int,
	callData *string,
	txType TransactionType,
	status TransactionStatus,
	note string,
) EvmTransaction {
	return EvmTransaction{
		UserID:      userId,
		Hash:        hash,
		Chain_id:    chainID,
		Nonce:       nonce,
		FromAddress: &fromAddress,
		ToAddress:   toAddress,
		Value:       value,
		CallData:    callData,
		Type:        txType,
		Status:      status,
		Note:        &note,
	}
}
