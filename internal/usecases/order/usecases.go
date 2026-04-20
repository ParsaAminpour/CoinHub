package order_usercases

import "coinhub/internal/domain/repositories"

type OrderUsecases struct {
	txManager repositories.TxManager
}

func NewOrderUsecases(txManager repositories.TxManager) *OrderUsecases {
	return &OrderUsecases{
		txManager: txManager,
	}
}
