package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// HTTP layer
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_http_requests_total",
			Help: "Total number of HTTP requests by method, path and status code.",
		},
		[]string{"method", "path", "status"},
	)

	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "coinhub_http_request_duration_seconds",
			Help:    "HTTP request latency distributions.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"method", "path"},
	)

	// Order engine
	OrdersSubmittedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_orders_submitted_total",
			Help: "Total orders submitted to the matching engine.",
		},
		[]string{"pair", "type", "side"},
	)

	OrdersMatchedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_orders_matched_total",
			Help: "Total orders fully or partially matched.",
		},
		[]string{"pair"},
	)

	OrdersCancelledTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_orders_cancelled_total",
			Help: "Total orders cancelled.",
		},
		[]string{"pair"},
	)

	OrdersExpiredTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_orders_expired_total",
			Help: "Total orders expired and removed by the reaper.",
		},
		[]string{"pair"},
	)

	ReaperRemovalFailedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_reaper_removal_failed_total",
			Help: "Total reaper removal attempts where the order was not found in the orderbook (already filled or cancelled).",
		},
		[]string{"pair"},
	)

	TradesExecutedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_trades_executed_total",
			Help: "Total number of trades executed (each fill event).",
		},
		[]string{"pair"},
	)

	// Kafka
	KafkaEventsPublishedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_kafka_events_published_total",
			Help: "Total Kafka events published by topic.",
		},
		[]string{"topic"},
	)

	KafkaEventsConsumedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_kafka_events_consumed_total",
			Help: "Total Kafka events consumed by topic.",
		},
		[]string{"topic"},
	)

	// Rate limiter
	RateLimitedRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "coinhub_rate_limited_requests_total",
			Help: "Total requests rejected by the rate limiter.",
		},
		[]string{"ip"},
	)

	// WebSocket
	WebSocketConnectionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "coinhub_websocket_connections_active",
			Help: "Current number of active WebSocket connections.",
		},
	)
)

var initOnce sync.Once

// Init registers all custom metrics. Safe to call multiple times (e.g. in tests) —
// registration only happens once. The Go runtime and process collectors are already
// registered by the default Prometheus registry and must not be added again.
func Init() {
	initOnce.Do(func() {
		prometheus.MustRegister(
			HTTPRequestsTotal,
			HTTPRequestDuration,
			OrdersSubmittedTotal,
			OrdersMatchedTotal,
			OrdersCancelledTotal,
			OrdersExpiredTotal,
			ReaperRemovalFailedTotal,
			TradesExecutedTotal,
			KafkaEventsPublishedTotal,
			KafkaEventsConsumedTotal,
			RateLimitedRequestsTotal,
			WebSocketConnectionsActive,
		)
	})
}
