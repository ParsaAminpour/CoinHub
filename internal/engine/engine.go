package engine

import (
	"coinhub/internal/domain/repositories"
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// this section will run a goroutine running a price-time priority match engine associated to each supported pairs

const (
	OrderChanBufferSize int64 = 10_000
)

// TODO : Complete it
type Engine interface {
	SubmitOrder(order Order) error
}

type MatchEngine struct {
	Orderbook Orderbook
	OrderChan chan Order // order comes to this channel via the associated HTTP handler
	TradeChan <-chan Trade
	quiteSig  chan struct{}
}

func NewMatchEngine() Engine {
	return &MatchEngine{
		OrderChan: make(chan Order, OrderChanBufferSize),
	}
}

func (me *MatchEngine) SubmitOrder(order Order) error {
	// called by the HTTP handler
	// pushing the order into orderCh
	me.OrderChan <- order
	return nil
}

// TODO : add this to the flow
func (me *MatchEngine) OrderRouter(order Order) error

func (me *MatchEngine) OrderDispatcher(kafkaClient *kgo.Client) error {
	// runs for each pair
	// it gets the order from the orderCh
	// calls the Match or Cancel based on the type of order dequeued from the channel.
	for incomingOrder := range me.OrderChan {
		switch incomingOrder.Type {
		case OrderTypeMarket:
			if err := me.Orderbook.MatchMarket(incomingOrder, kafkaClient); err != nil {
				continue
			}
		case OrderTypeLimit:
			if err := me.Orderbook.MatchLimit(incomingOrder); err != nil {
				continue
			}
		case OrderTypeCancel:
			if err := me.Orderbook.Cancel(incomingOrder); err != nil {
				continue
			}
		}
	}
	return nil
}

func (pl *PriceLevel) BestOrderInPriceLevel() (*Order, error) {
	return pl.Orders[len(pl.Orders)], nil
}

func SetupMatchEngine(ctx context.Context, assetRepository repositories.AssetRepository) error {
	assets, err := assetRepository.GetAvailableAssets(ctx)
	if err != nil {
		return err
	}
	for _, a := range assets {
		fmt.Print(a.ID)
	}
	return nil
}

// should return the trades, the orders that get matched and become filled or partial
// wire the channels and orderbook matching algorithm with eachothers.
func (me *MatchEngine) RunMatchEngineForPair(ctx context.Context) error {

	return nil
}
