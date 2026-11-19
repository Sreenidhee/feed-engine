# feed-engine

A distributed feed generation service modelled after Reddit/Twitter-style social feeds. Post and interaction events flow through Kafka; consumer workers fan out to per-user feed caches in Redis. The REST API serves feeds from Redis sorted sets for low-latency reads.

## Architecture

```
                ┌──────────────────────────────────────────────────────┐
                │                   API service :8080                  │
                │                                                      │
  POST /posts ──┼──► Postgres (authoritative) ──► Kafka: post.created  │
  POST /like  ──┼──────────────────────────────► Kafka: post.interaction│
  GET  /feed  ──┼──► Redis ZREVRANGE ──► Postgres (hydrate posts)       │
                └──────────────────────────────────────────────────────┘
                                   │ Kafka topics
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
          ┌─────────────────┐          ┌─────────────────┐
          │  consumer [1]   │          │  consumer [2]   │  ← same group,
          │  post.created   │          │  post.interact  │    partitions split
          │                 │          │                 │
          │ GetFollowers()  │          │ IncrLikes()     │
          │ Redis ZADD ×N   │          │                 │
          └────────┬────────┘          └────────┬────────┘
                   │                            │
                   ▼                            ▼
           Redis feed:{uid}             Postgres likes++
           ZSET score=unix_ts
```

### Key design decisions

| Decision | Why |
|---|---|
| Kafka fan-out (not synchronous) | A post by a user with 10k followers would block the API for seconds if fan-out were synchronous; Kafka decouples it |
| Redis sorted set per user | O(log N) write per follower, O(log N + limit) read; no full table scan on feed load |
| Feed trimmed to 1 000 entries | Prevents unbounded Redis memory growth; old posts fall off naturally |
| Idempotent consumers | Kafka delivers at-least-once; `processed_events` table with `ON CONFLICT DO NOTHING` makes every event safe to replay |
| Two consumer replicas | Kafka distributes partitions across the group — horizontal scale without code changes |

## Endpoints

| Method | Path | Body | Description |
|---|---|---|---|
| `POST` | `/users` | `{"username":"alice"}` | Create user |
| `GET` | `/users/{id}` | — | Get user |
| `POST` | `/users/{id}/follow` | `{"target_id":2}` | Follow a user |
| `POST` | `/posts` | `{"user_id":1,"content":"..."}` | Create post → Kafka |
| `GET` | `/posts/{id}` | — | Get post |
| `POST` | `/posts/{id}/like` | `{"user_id":2}` | Like post → Kafka |
| `GET` | `/feed/{user_id}` | `?limit=20&offset=0` | Read feed from Redis |
| `GET` | `/health` | — | Health check |

## Run with Docker Compose

```bash
make up       # starts Postgres, Redis, Kafka, api, 2× consumer
make seed     # creates users, follow, post, reads Bob's feed
make down     # tears everything down
```

Requires Docker with Compose v2 (`docker compose`).

## Run locally

```bash
# Dependencies running somewhere (edit .env if needed)
export POSTGRES_DSN=postgres://feed:feed@localhost:5432/feed?sslmode=disable
export REDIS_ADDR=localhost:6379
export KAFKA_BROKER=localhost:9092

go mod tidy

# Terminal 1
go run ./cmd/api

# Terminal 2
go run ./cmd/consumer
```

## Schema

```sql
users            — id, username, created_at
follows          — follower_id, followee_id  (PK composite)
posts            — id, user_id, content, likes, created_at
processed_events — event_id (idempotency log)
```

## Simulate production load

```bash
# Create 100 users, have them all follow user 1, post 50 times, read feeds
for i in $(seq 2 100); do
  curl -s -X POST localhost:8080/users -d "{\"username\":\"user$i\"}" > /dev/null
  curl -s -X POST localhost:8080/users/$i/follow -d '{"target_id":1}' > /dev/null
done
for i in $(seq 1 50); do
  curl -s -X POST localhost:8080/posts \
    -d "{\"user_id\":1,\"content\":\"post $i\"}" > /dev/null
done
curl -s "localhost:8080/feed/2?limit=50" | jq '.total'
```

## Tech

Go 1.21 · gorilla/mux · segmentio/kafka-go · go-redis/v9 · jackc/pgx/v5 · Docker Compose · Kafka (KRaft) · Redis 7 · PostgreSQL 16
