package kafka

import (
	"context"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Message = kafka.Message

type Consumer struct{ r *kafka.Reader }

func NewConsumer(topic, groupID string) *Consumer {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	return &Consumer{r: kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{broker},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       1 << 20, // 1 MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})}
}

// ReadMessage blocks until a message is available.
func (c *Consumer) ReadMessage(ctx context.Context) (Message, error) {
	return c.r.ReadMessage(ctx)
}

func (c *Consumer) Close() error { return c.r.Close() }
