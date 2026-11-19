GOFLAGS := -trimpath

.PHONY: all build api consumer up down tidy seed

all: build

build: api consumer

api:
	go build $(GOFLAGS) -o bin/api ./cmd/api

consumer:
	go build $(GOFLAGS) -o bin/consumer ./cmd/consumer

tidy:
	go mod tidy

up:
	docker compose up --build -d
	@echo "API → http://localhost:8080"

down:
	docker compose down -v

# Quick smoke-test: create two users, follow, post, check feed
seed:
	@echo "Creating users..."
	@curl -s -X POST localhost:8080/users -d '{"username":"alice"}' | jq .
	@curl -s -X POST localhost:8080/users -d '{"username":"bob"}'   | jq .
	@echo "\nBob (id=2) follows Alice (id=1)..."
	@curl -s -X POST localhost:8080/users/2/follow -d '{"target_id":1}' | jq .
	@echo "\nAlice posts..."
	@curl -s -X POST localhost:8080/posts -d '{"user_id":1,"content":"Hello from Alice!"}' | jq .
	@echo "\nBob'\''s feed (should contain Alice'\''s post)..."
	@sleep 1
	@curl -s localhost:8080/feed/2 | jq .
