DO $migration$
BEGIN
  ALTER TABLE subscriptions ADD COLUMN lock_generation bigint NOT NULL DEFAULT 0;

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 5, 0);
END;
$migration$ LANGUAGE plpgsql;
