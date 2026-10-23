# Build Stage
FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /bin/faultwire ./cmd/faultwire

# Production Runtime Stage
FROM scratch

COPY --from=builder /bin/faultwire /faultwire

EXPOSE 8000 8081

ENTRYPOINT ["/faultwire"]