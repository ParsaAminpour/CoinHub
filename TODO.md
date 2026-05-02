# TODO

Feature backlog and known tasks for CoinHub.

---

## Features

- [x] Implement Admin Panel — back-office interface for the CEX to manage users, assets, orders, and system configuration
- [ ] Implement Critical Resolver — internal subsystem for detecting and managing critical system events (e.g. circuit breakers, fraud signals, anomaly alerts, emergency halts)
- [ ] Asset info HTTP handlers — public routes to query a single asset and list all assets with their network availability, trading pairs, and status (`internal/adapter/handler/http/asset.go`)
- [ ] Multi-network support — config currently handles only two networks simultaneously (`internal/infrastructure/configs/configs.go:54`)
- [x] Use a B-Tree instead of slices for the order price level in the order book for better performance (`internal/engine/orderbook.go:23`)
- [ ] Graceful shutdown for all connections (DB, Kafka, WebSocket) (`internal/application.go:61`)
- [ ] Determine and implement the `expired` order status flow (`internal/adapter/messaging/kafka/event.go:29`)

## Security

- [ ] Lock down WebSocket CORS — remove `InsecureSkipVerify: true` and restrict to actual origins in production (`internal/adapter/handler/http/router.go:71`)
- [ ] Add allowed origins list for CORS (`internal/adapter/handler/http/router.go:114`)
- [ ] Set trusted proxy addresses (load balancer IP) (`internal/adapter/handler/http/router.go:113`)
- [ ] Integrate Sentry for crash/error reporting in production (`internal/adapter/handler/http/router.go:104`)

## Cleanup

- [ ] Remove unused `WsClient *websocket.Conn` field from application struct if not needed (`internal/application.go:98`)
- [ ] Set a proper nonce value in wallet transfer (`internal/usecases/wallet_usecases/transfer.go:106`)
