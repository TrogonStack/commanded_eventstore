DO $migration$
BEGIN
  -- Unreleased builds of the fork added the lock generation and recorded it as
  -- schema version 1.5.0, which would hide upstream migrations up to 1.5.0
  -- from upstream's migrate.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
    AND table_name = 'subscriptions'
    AND column_name = 'lock_generation'
  ) THEN
    DELETE FROM schema_migrations
    WHERE (major_version, minor_version, patch_version) = (1, 5, 0);

    -- Event stores those builds created recorded 1.5.0 alone, and an empty
    -- history makes upstream's migrate run every migration again.
    IF NOT EXISTS (SELECT 1 FROM schema_migrations) THEN
      INSERT INTO schema_migrations (major_version, minor_version, patch_version)
      VALUES (1, 3, 2);
    END IF;
  ELSE
    ALTER TABLE subscriptions ADD COLUMN lock_generation bigint NOT NULL DEFAULT 0;
  END IF;

  INSERT INTO fork_schema_migrations (name) VALUES ('subscription_lock_generation');
END;
$migration$ LANGUAGE plpgsql;
