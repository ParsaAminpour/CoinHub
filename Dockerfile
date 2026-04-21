# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

# git is needed by some Go modules at build time
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

# Cache dependencies before copying source (layer cache optimization)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a single static binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o coinhub .

# ── Stage 2: final image ───────────────────────────────────────────────────────
FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/coinhub .

# The app tries to load .env from the working directory.
# In Docker we skip the file and rely on environment variables instead
# (cleanenv falls back to env vars automatically when the file is missing).

# Expose API port
EXPOSE 8083

# Default command — overridden per-service in docker-compose
ENTRYPOINT ["./coinhub"]
CMD ["api"]
