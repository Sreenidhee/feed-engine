package cache

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const maxFeedSize = 1000 // trim feed to this after writes

type Cache struct{ rdb *redis.Client }

func New() *Cache {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return &Cache{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (c *Cache) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func feedKey(userID int) string {
	return fmt.Sprintf("feed:%d", userID)
}

// AddToFeed adds postID to userID's feed with the given score (Unix timestamp).
// Trims the feed to maxFeedSize entries so memory stays bounded.
func (c *Cache) AddToFeed(ctx context.Context, userID, postID int, score float64) error {
	key := feedKey(userID)
	pipe := c.rdb.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: strconv.Itoa(postID)})
	// Keep only the top maxFeedSize newest entries
	pipe.ZRemRangeByRank(ctx, key, 0, int64(-maxFeedSize-1))
	_, err := pipe.Exec(ctx)
	return err
}

// GetFeed returns up to limit post IDs from userID's feed, newest first.
// offset enables pagination.
func (c *Cache) GetFeed(ctx context.Context, userID, offset, limit int) ([]int, error) {
	key := feedKey(userID)
	// ZREVRANGE returns highest scores (newest) first
	vals, err := c.rdb.ZRevRange(ctx, key, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, err
	}
	ids := make([]int, 0, len(vals))
	for _, v := range vals {
		id, _ := strconv.Atoi(v)
		ids = append(ids, id)
	}
	return ids, nil
}

// FeedLen returns the number of items in a user's cached feed.
func (c *Cache) FeedLen(ctx context.Context, userID int) (int64, error) {
	return c.rdb.ZCard(ctx, feedKey(userID)).Result()
}
