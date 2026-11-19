package models

import "time"

// Domain types

type User struct {
	ID        int       `json:"id"`
	Username  string    `json:"username"`
	CreatedAt time.Time `json:"created_at"`
}

type Post struct {
	ID        int       `json:"id"`
	UserID    int       `json:"user_id"`
	Content   string    `json:"content"`
	Likes     int       `json:"likes"`
	CreatedAt time.Time `json:"created_at"`
}

type FeedItem struct {
	PostID    int       `json:"post_id"`
	UserID    int       `json:"user_id"`
	Content   string    `json:"content"`
	Likes     int       `json:"likes"`
	Score     float64   `json:"score"`
	CreatedAt time.Time `json:"created_at"`
}

// Kafka event types

const (
	EventPostCreated     = "post.created"
	EventPostInteraction = "post.interaction"
)

type PostCreatedEvent struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	UserID    int       `json:"user_id"`
	PostID    int       `json:"post_id"`
	Timestamp time.Time `json:"timestamp"`
}

type InteractionEvent struct {
	EventID   string    `json:"event_id"`
	Type      string    `json:"type"`
	UserID    int       `json:"user_id"`
	PostID    int       `json:"post_id"`
	Action    string    `json:"action"` // "like" | "comment"
	Timestamp time.Time `json:"timestamp"`
}
