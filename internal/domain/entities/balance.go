package entities

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// NOTE : The `Quantity` field of this table will alter in three scenario:
//  1. with onchain transfer
//  2. service's users internal transfer
//  3. orders that get filled and a trade event emitted
type AssetBalance struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey;default:gen_random_uuid()"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	UserID uuid.UUID `gorm:"type:uuid;index;not null"`
	User   User      `gorm:"foreignKey:UserID"`

	Asset    Asset `gorm:"foreignkey:BalanceID"`
	Quantity decimal.Decimal
}
