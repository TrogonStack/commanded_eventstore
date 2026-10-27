ALTER TABLE subscriptions ADD COLUMN lock_generation bigint NOT NULL DEFAULT 0;
