FROM golang:1.27-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /app/raftkv-server ./cmd/raftkv-server
RUN CGO_ENABLED=0 go build -o /app/raftkv-cli ./cmd/raftkv-cli

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/raftkv-server /app/raftkv-server
COPY --from=builder /app/raftkv-cli /app/raftkv-cli
COPY configs/ /app/configs/

EXPOSE 50051 50052 50053
ENTRYPOINT ["/app/raftkv-server"]
