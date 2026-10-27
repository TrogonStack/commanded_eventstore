DO $migration$
BEGIN
  -- Earlier fork releases added the lock generation and recorded it as schema
  -- version 1.5.0, which would hide upstream migrations up to 1.5.0 from
  -- upstream's migrate.
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema()
    AND table_name = 'subscriptions'
    AND column_name = 'lock_generation'
  ) THEN
    DELETE FROM schema_migrations
    WHERE (major_version, minor_version, patch_version) = (1, 5, 0);
  ELSE
    ALTER TABLE subscriptions ADD COLUMN lock_generation bigint NOT NULL DEFAULT 0;
  END IF;

  INSERT INTO fork_schema_migrations (name) VALUES ('subscription_lock_generation');
END;
$migration$ LANGUAGE plpgsql;
