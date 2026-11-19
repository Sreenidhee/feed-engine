package fanout

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/S-HEMANTH-REDDY/feed-engine/internal/cache"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/db"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/models"
)

// Worker processes Kafka messages and fans post events out to follower feeds.
type Worker struct {
	db    *db.DB
	cache *cache.Cache
}

func NewWorker(d *db.DB, c *cache.Cache) *Worker {
	return &Worker{db: d, cache: c}
}

func (w *Worker) HandlePostCreated(ctx context.Context, raw []byte) error {
	var ev models.PostCreatedEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("unmarshal post.created: %w", err)
	}

	// Idempotency check: skip if we've already processed this event
	inserted, err := w.db.MarkProcessed(ctx, ev.EventID)
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	if !inserted {
		log.Printf("[fanout] skip duplicate event %s", ev.EventID)
		return nil
	}

	followers, err := w.db.GetFollowers(ctx, ev.UserID)
	if err != nil {
		return fmt.Errorf("get followers for user %d: %w", ev.UserID, err)
	}

	score := float64(ev.Timestamp.Unix())
	fanoutCount := 0
	for _, followerID := range followers {
		if err := w.cache.AddToFeed(ctx, followerID, ev.PostID, score); err != nil {
			log.Printf("[fanout] add to feed user=%d post=%d: %v", followerID, ev.PostID, err)
			continue
		}
		fanoutCount++
	}

	log.Printf("[fanout] post=%d user=%d fanned-out to %d/%d followers",
		ev.PostID, ev.UserID, fanoutCount, len(followers))
	return nil
}

func (w *Worker) HandleInteraction(ctx context.Context, raw []byte) error {
	var ev models.InteractionEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return fmt.Errorf("unmarshal post.interaction: %w", err)
	}

	inserted, err := w.db.MarkProcessed(ctx, ev.EventID)
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	if !inserted {
		log.Printf("[fanout] skip duplicate interaction event %s", ev.EventID)
		return nil
	}

	if ev.Action == "like" {
		if err := w.db.IncrementLikes(ctx, ev.PostID); err != nil {
			return fmt.Errorf("increment likes post=%d: %w", ev.PostID, err)
		}
		log.Printf("[fanout] like post=%d by user=%d", ev.PostID, ev.UserID)
	}

	return nil
}
