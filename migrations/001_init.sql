CREATE TABLE IF NOT EXISTS users (
    id         SERIAL PRIMARY KEY,
    username   TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS follows (
    follower_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    followee_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (follower_id, followee_id)
);
CREATE INDEX IF NOT EXISTS idx_follows_followee ON follows(followee_id);

CREATE TABLE IF NOT EXISTS posts (
    id         SERIAL PRIMARY KEY,
    user_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content    TEXT NOT NULL,
    likes      INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id, created_at DESC);

-- Idempotency table: consumers mark events as processed here.
-- ON CONFLICT DO NOTHING prevents double-processing on consumer restart.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
