DO $migration$
DECLARE
  -- Builds before the fork migration track added lock_generation and recorded
  -- this migration as 1.5.0 in the upstream history, where it would hide a
  -- future upstream 1.5.0 migration. Without the column, a 1.5.0 row there is
  -- upstream's own.
  written_by_fork boolean := EXISTS (
    SELECT 1 FROM pg_attribute
    WHERE attrelid = 'subscriptions'::regclass
    AND attname = 'lock_generation'
    AND NOT attisdropped
  );
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
  IF written_by_fork THEN
    INSERT INTO trogon_schema_migrations (major_version, minor_version, patch_version, migrated_at)
    SELECT 1, 5, 0, COALESCE(
      (SELECT migrated_at FROM schema_migrations
       WHERE major_version = 1 AND minor_version = 5 AND patch_version = 0),
      NOW()
    );

    DELETE FROM schema_migrations WHERE major_version = 1 AND minor_version = 5 AND patch_version = 0;
  ELSE
    INSERT INTO trogon_schema_migrations (major_version, minor_version, patch_version)
    VALUES (1, 5, 0);
  END IF;
END;
$migration$ LANGUAGE plpgsql;
