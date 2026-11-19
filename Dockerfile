FROM golang:1.21-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /bin/api      ./cmd/api
RUN go build -o /bin/consumer ./cmd/consumer

FROM alpine:3.19 AS api
COPY --from=builder /bin/api /bin/api
COPY migrations/ /app/migrations/
WORKDIR /app
ENTRYPOINT ["/bin/api"]

FROM alpine:3.19 AS consumer
COPY --from=builder /bin/consumer /bin/consumer
ENTRYPOINT ["/bin/consumer"]
