package db

import (
	"context"
	"os"

	"github.com/S-HEMANTH-REDDY/feed-engine/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct{ pool *pgxpool.Pool }

func New(ctx context.Context) (*DB, error) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://feed:feed@localhost:5432/feed?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &DB{pool: pool}, pool.Ping(ctx)
}

func (d *DB) Close() { d.pool.Close() }

// RunMigrations reads the SQL file at path and executes it.
func (d *DB) RunMigrations(ctx context.Context, sql string) error {
	content, err := os.ReadFile(sql)
	if err != nil {
		return err
	}
	_, err = d.pool.Exec(ctx, string(content))
	return err
}

// --- Users ---

func (d *DB) CreateUser(ctx context.Context, username string) (*models.User, error) {
	u := &models.User{}
	err := d.pool.QueryRow(ctx,
		`INSERT INTO users (username) VALUES ($1)
		 RETURNING id, username, created_at`, username).
		Scan(&u.ID, &u.Username, &u.CreatedAt)
	return u, err
}

func (d *DB) GetUser(ctx context.Context, id int) (*models.User, error) {
	u := &models.User{}
	err := d.pool.QueryRow(ctx,
		`SELECT id, username, created_at FROM users WHERE id=$1`, id).
		Scan(&u.ID, &u.Username, &u.CreatedAt)
	return u, err
}

// --- Follows ---

func (d *DB) Follow(ctx context.Context, followerID, followeeID int) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO follows (follower_id, followee_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
		followerID, followeeID)
	return err
}

// GetFollowers returns IDs of users who follow userID.
func (d *DB) GetFollowers(ctx context.Context, userID int) ([]int, error) {
	rows, err := d.pool.Query(ctx,
		`SELECT follower_id FROM follows WHERE followee_id=$1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// --- Posts ---

func (d *DB) CreatePost(ctx context.Context, userID int, content string) (*models.Post, error) {
	p := &models.Post{}
	err := d.pool.QueryRow(ctx,
		`INSERT INTO posts (user_id, content) VALUES ($1,$2)
		 RETURNING id, user_id, content, likes, created_at`, userID, content).
		Scan(&p.ID, &p.UserID, &p.Content, &p.Likes, &p.CreatedAt)
	return p, err
}

func (d *DB) GetPost(ctx context.Context, id int) (*models.Post, error) {
	p := &models.Post{}
	err := d.pool.QueryRow(ctx,
		`SELECT id, user_id, content, likes, created_at FROM posts WHERE id=$1`, id).
		Scan(&p.ID, &p.UserID, &p.Content, &p.Likes, &p.CreatedAt)
	return p, err
}

func (d *DB) IncrementLikes(ctx context.Context, postID int) error {
	_, err := d.pool.Exec(ctx, `UPDATE posts SET likes=likes+1 WHERE id=$1`, postID)
	return err
}

// GetPostsByIDs returns posts for the given IDs, preserving order.
func (d *DB) GetPostsByIDs(ctx context.Context, ids []int) (map[int]*models.Post, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	rows, err := d.pool.Query(ctx,
		`SELECT id, user_id, content, likes, created_at FROM posts WHERE id=ANY($1)`,
		ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[int]*models.Post, len(ids))
	for rows.Next() {
		p := &models.Post{}
		if err := rows.Scan(&p.ID, &p.UserID, &p.Content, &p.Likes, &p.CreatedAt); err != nil {
			return nil, err
		}
		out[p.ID] = p
	}
	return out, rows.Err()
}

// --- Idempotency ---

// MarkProcessed inserts event_id; returns false if already processed.
func (d *DB) MarkProcessed(ctx context.Context, eventID string) (bool, error) {
	tag, err := d.pool.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}
