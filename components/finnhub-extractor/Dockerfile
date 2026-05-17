FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy entire project (required for replace directives)
COPY . .

WORKDIR /app/components/finnhub-extractor

# Tidy dependencies
RUN go mod tidy

# Build the component with cache mounts
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o /finnhub-extractor .

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /finnhub-extractor /app/finnhub-extractor

# Run as non-root user
RUN adduser -D -u 1000 datuplet
USER datuplet

ENTRYPOINT ["/app/finnhub-extractor"]
