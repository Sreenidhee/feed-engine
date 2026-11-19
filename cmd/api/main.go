package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/S-HEMANTH-REDDY/feed-engine/internal/cache"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/db"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/kafka"
	"github.com/S-HEMANTH-REDDY/feed-engine/internal/models"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type server struct {
	db       *db.DB
	cache    *cache.Cache
	producer *kafka.Producer
}

// ---- helpers ----------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

func pathInt(r *http.Request, key string) (int, error) {
	return strconv.Atoi(mux.Vars(r)[key])
}

// ---- handlers ---------------------------------------------------------------

// POST /users   body: {"username":"alice"}
func (s *server) createUser(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Username == "" {
		writeErr(w, http.StatusBadRequest, "username required"); return
	}
	u, err := s.db.CreateUser(r.Context(), body.Username)
	if err != nil {
		writeErr(w, http.StatusConflict, "username already taken"); return
	}
	writeJSON(w, http.StatusCreated, u)
}

// GET /users/{id}
func (s *server) getUser(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid user id"); return
	}
	u, err := s.db.GetUser(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "user not found"); return
	}
	writeJSON(w, http.StatusOK, u)
}

// POST /users/{id}/follow   body: {"target_id":2}
func (s *server) followUser(w http.ResponseWriter, r *http.Request) {
	followerID, err := pathInt(r, "id")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid user id"); return
	}
	var body struct {
		TargetID int `json:"target_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TargetID == 0 {
		writeErr(w, http.StatusBadRequest, "target_id required"); return
	}
	if err := s.db.Follow(r.Context(), followerID, body.TargetID); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error()); return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "following"})
}

// POST /posts   body: {"user_id":1,"content":"hello"}
func (s *server) createPost(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID  int    `json:"user_id"`
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Content == "" || body.UserID == 0 {
		writeErr(w, http.StatusBadRequest, "user_id and content required"); return
	}
	p, err := s.db.CreatePost(r.Context(), body.UserID, body.Content)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error()); return
	}

	ev := models.PostCreatedEvent{
		EventID:   uuid.New().String(),
		Type:      models.EventPostCreated,
		UserID:    p.UserID,
		PostID:    p.ID,
		Timestamp: p.CreatedAt,
	}
	if err := s.producer.Publish(r.Context(), models.EventPostCreated,
		strconv.Itoa(p.ID), ev); err != nil {
		log.Printf("[api] kafka publish post.created: %v", err)
		// Non-fatal: post is saved; fan-out will lag but won't be lost on retry
	}
	writeJSON(w, http.StatusCreated, p)
}

// GET /posts/{id}
func (s *server) getPost(w http.ResponseWriter, r *http.Request) {
	id, err := pathInt(r, "id")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid post id"); return
	}
	p, err := s.db.GetPost(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "post not found"); return
	}
	writeJSON(w, http.StatusOK, p)
}

// POST /posts/{id}/like   body: {"user_id":2}
func (s *server) likePost(w http.ResponseWriter, r *http.Request) {
	postID, err := pathInt(r, "id")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid post id"); return
	}
	var body struct {
		UserID int `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == 0 {
		writeErr(w, http.StatusBadRequest, "user_id required"); return
	}

	ev := models.InteractionEvent{
		EventID:   uuid.New().String(),
		Type:      models.EventPostInteraction,
		UserID:    body.UserID,
		PostID:    postID,
		Action:    "like",
		Timestamp: time.Now(),
	}
	if err := s.producer.Publish(r.Context(), models.EventPostInteraction,
		fmt.Sprintf("%d-%d", postID, body.UserID), ev); err != nil {
		log.Printf("[api] kafka publish post.interaction: %v", err)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "liked"})
}

// GET /feed/{user_id}?limit=20&offset=0
func (s *server) getFeed(w http.ResponseWriter, r *http.Request) {
	userID, err := pathInt(r, "user_id")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid user id"); return
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	postIDs, err := s.cache.GetFeed(r.Context(), userID, offset, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error()); return
	}

	if len(postIDs) == 0 {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "total": 0})
		return
	}

	posts, err := s.db.GetPostsByIDs(r.Context(), postIDs)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error()); return
	}

	// Return feed in the same order as Redis (newest first)
	items := make([]models.FeedItem, 0, len(postIDs))
	for _, id := range postIDs {
		p, ok := posts[id]
		if !ok {
			continue
		}
		items = append(items, models.FeedItem{
			PostID:    p.ID,
			UserID:    p.UserID,
			Content:   p.Content,
			Likes:     p.Likes,
			Score:     float64(p.CreatedAt.Unix()),
			CreatedAt: p.CreatedAt,
		})
	}

	total, _ := s.cache.FeedLen(r.Context(), userID)
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": total})
}

// GET /health
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- main -------------------------------------------------------------------

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	database, err := db.New(ctx)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer database.Close()

	if err := database.RunMigrations(ctx, "migrations/001_init.sql"); err != nil {
		log.Fatalf("migrations: %v", err)
	}
	log.Println("[api] migrations applied")

	redisCache := cache.New()
	if err := redisCache.Ping(ctx); err != nil {
		log.Fatalf("redis: %v", err)
	}

	producer := kafka.NewProducer()
	defer producer.Close()

	s := &server{db: database, cache: redisCache, producer: producer}

	r := mux.NewRouter()
	r.HandleFunc("/health", healthHandler).Methods(http.MethodGet)
	r.HandleFunc("/users", s.createUser).Methods(http.MethodPost)
	r.HandleFunc("/users/{id:[0-9]+}", s.getUser).Methods(http.MethodGet)
	r.HandleFunc("/users/{id:[0-9]+}/follow", s.followUser).Methods(http.MethodPost)
	r.HandleFunc("/posts", s.createPost).Methods(http.MethodPost)
	r.HandleFunc("/posts/{id:[0-9]+}", s.getPost).Methods(http.MethodGet)
	r.HandleFunc("/posts/{id:[0-9]+}/like", s.likePost).Methods(http.MethodPost)
	r.HandleFunc("/feed/{user_id:[0-9]+}", s.getFeed).Methods(http.MethodGet)

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{Addr: ":" + port, Handler: r}

	go func() {
		log.Printf("[api] listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[api] shutting down...")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
}
