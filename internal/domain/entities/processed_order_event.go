package entities

import "time"

type ProcessedOrderEvent struct {
	ID        uint   `gorm:"primaryKey"`
	EventID   string `gorm:"uniqueIndex;size:191"`
	Consumer  string `gorm:"index;size:191"`
	CreatedAt time.Time
}

func NewProcessedOrderEvent(eventID, consumerName string) *ProcessedOrderEvent {
	return &ProcessedOrderEvent{
		EventID:  eventID,
		Consumer: consumerName,
	}
}
