package kafka

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct{ w *kafka.Writer }

func NewProducer() *Producer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	return &Producer{w: &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 5 * time.Second,
		RequiredAcks: kafka.RequireOne,
		// Automatic topic creation is enabled in docker-compose
	}}
}

func (p *Producer) Publish(ctx context.Context, topic string, key string, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	})
}

func (p *Producer) Close() error { return p.w.Close() }
