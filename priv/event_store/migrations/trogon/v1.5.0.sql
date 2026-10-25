DO $migration$
BEGIN
  CREATE TABLE IF NOT EXISTS trogon_schema_migrations
  (
      major_version int NOT NULL,
      minor_version int NOT NULL,
      patch_version int NOT NULL,
      migrated_at timestamp with time zone DEFAULT NOW() NOT NULL,
      PRIMARY KEY(major_version, minor_version, patch_version)
  );

  ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS lock_generation bigint NOT NULL DEFAULT 0;

  -- Record schema migration
  INSERT INTO trogon_schema_migrations (major_version, minor_version, patch_version) VALUES (1, 5, 0);
END;
$migration$ LANGUAGE plpgsql;
