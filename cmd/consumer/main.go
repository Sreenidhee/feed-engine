package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/S-HEMANTH-REDDY/feed-engine/internal/cache"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/db"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/fanout"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/kafka"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/models"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	database, err := db.New(ctx)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer database.Close()

	redisCache := cache.New()
	if err := redisCache.Ping(ctx); err != nil {
		log.Fatalf("redis: %v", err)
	}

	worker := fanout.NewWorker(database, redisCache)

	var wg sync.WaitGroup

	// Consumer for post.created events
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := kafka.NewConsumer(models.EventPostCreated, "feed-consumer-posts")
		defer c.Close()
		log.Println("[consumer] listening on topic:", models.EventPostCreated)
		for {
			msg, err := c.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return // shutdown
				}
				log.Printf("[consumer] read error: %v", err)
				continue
			}
			if err := worker.HandlePostCreated(ctx, msg.Value); err != nil {
				log.Printf("[consumer] handle post.created: %v", err)
			}
		}
	}()

	// Consumer for post.interaction events
	wg.Add(1)
	go func() {
		defer wg.Done()
		c := kafka.NewConsumer(models.EventPostInteraction, "feed-consumer-interactions")
		defer c.Close()
		log.Println("[consumer] listening on topic:", models.EventPostInteraction)
		for {
			msg, err := c.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[consumer] read error: %v", err)
				continue
			}
			if err := worker.HandleInteraction(ctx, msg.Value); err != nil {
				log.Printf("[consumer] handle post.interaction: %v", err)
			}
		}
	}()

	log.Println("[consumer] started — waiting for events (Ctrl+C to stop)")
	wg.Wait()
	log.Println("[consumer] shutdown complete")
}
