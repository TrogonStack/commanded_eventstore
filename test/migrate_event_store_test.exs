defmodule EventStore.MigrateEventStoreTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  alias EventStore.Storage.Database

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    [config: config]
  end

  describe "migrate event store" do
    test "should prevent migrations from running concurrently", %{config: config} do
      restore_pre_migration_event_store(config)

      log =
        [
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end)
        ]
        |> Enum.map(&Task.await/1)
        |> Enum.sort()

      # One migration should succeed, two should be prevented due to already running
      assert Enum.at(log, 0) =~ "EventStore database migration already in progress."
      assert Enum.at(log, 1) =~ "EventStore database migration already in progress."
      assert Enum.at(log, 2) =~ "The EventStore database has been migrated."

      assert length(log) == 3
    end
  end

  describe "migrated event store schema" do
    test "should be identical to new event store schema", %{config: config} do
      restore_pre_migration_event_store(config)
      migrate_eventstore(config, quiet: true, is_mix: false)

      # Dump migrated database
      {migrated_schema, 0} = Database.dump(config, [])

      create_new_database(config)

      # Dump newly created database
      {new_schema, 0} = Database.dump(config, [])

      migrated_normalized = split_and_normalize(migrated_schema)
      new_normalized = split_and_normalize(new_schema)

      [migrated_normalized, new_normalized]
      |> Enum.zip()
      |> Enum.each(fn {migrated, new} ->
        assert migrated == new
      end)

      assert length(migrated_normalized) == length(new_normalized)
    end
  end

  describe "migrate an event store in its own schema" do
    test "should leave other event stores in the same database untouched", %{config: config} do
      restore_pre_migration_event_store(config)

      with_conn(config, fn conn ->
        Postgrex.query!(conn, ~s(ALTER SCHEMA public RENAME TO "example"), [])
        Postgrex.query!(conn, "CREATE SCHEMA public", [])

        EventStore.Storage.Initializer.run!(conn, config)
      end)

      public_history = migration_history(config, "public")

      migrate_eventstore(Keyword.put(config, :schema, "example"), quiet: true, is_mix: false)

      assert has_lock_generation?(config, "example")
      assert "1.3.2" == latest_version(config, "example")
      assert public_history == migration_history(config, "public")
    end
  end

  describe "event store created by the fork" do
    test "should record no schema version above upstream's", %{config: config} do
      create_new_database(config)

      assert "1.3.2" == latest_version(config, "public")
    end
  end

  describe "migrate an event store the fork recorded as 1.5.0" do
    test "should leave no schema version above upstream's", %{config: config} do
      create_new_database(config)

      with_conn(config, fn conn ->
        Postgrex.query!(
          conn,
          "INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 5, 0)",
          []
        )
      end)

      migrate_eventstore(config, quiet: true, is_mix: false)

      assert has_lock_generation?(config, "public")
      assert "1.3.2" == latest_version(config, "public")
    end
  end

  describe "migrate an event store that recorded upstream's 1.5.0" do
    test "should still add the lock generation", %{config: config} do
      restore_pre_migration_event_store(config)

      with_conn(config, fn conn ->
        Postgrex.query!(
          conn,
          "INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 5, 0)",
          []
        )
      end)

      migrate_eventstore(config, quiet: true, is_mix: false)

      assert has_lock_generation?(config, "public")
    end

    test "should keep upstream's 1.5.0 when migrated again", %{config: config} do
      create_new_database(config)

      with_conn(config, fn conn ->
        Postgrex.query!(conn, "ALTER TABLE subscriptions DROP COLUMN lock_generation", [])

        Postgrex.query!(
          conn,
          "INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 5, 0)",
          []
        )
      end)

      migrate_eventstore(config, quiet: true, is_mix: false)
      migrate_eventstore(config, quiet: true, is_mix: false)

      assert "1.5.0" == latest_version(config, "public")
    end
  end

  defp has_lock_generation?(config, schema) do
    with_conn(config, fn conn ->
      %Postgrex.Result{rows: [[exists]]} =
        Postgrex.query!(
          conn,
          """
          SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = $1
            AND table_name = 'subscriptions'
            AND column_name = 'lock_generation'
          )
          """,
          [schema]
        )

      exists
    end)
  end

  defp latest_version(config, schema) do
    with_conn(config, fn conn ->
      %Postgrex.Result{rows: [[version]]} =
        Postgrex.query!(
          conn,
          """
          SELECT concat_ws('.', major_version, minor_version, patch_version)
          FROM "#{schema}".schema_migrations
          ORDER BY major_version DESC, minor_version DESC, patch_version DESC
          LIMIT 1
          """,
          []
        )

      version
    end)
  end

  defp migration_history(config, schema) do
    with_conn(config, fn conn ->
      %Postgrex.Result{rows: rows} =
        Postgrex.query!(
          conn,
          ~s(SELECT * FROM "#{schema}".schema_migrations ORDER BY 1, 2, 3),
          []
        )

      rows
    end)
  end

  defp with_conn(config, fun) do
    {:ok, conn} = Postgrex.start_link(config)

    try do
      fun.(conn)
    after
      GenServer.stop(conn)
    end
  end

  defp split_and_normalize(enumerable) do
    enumerable
    |> Enum.join()
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.map(
      &Regex.replace(
        ~r/[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}\.[0-9]{0,6}\+[0-9]{2}/,
        &1,
        "NOW"
      )
    )
    # Ignore comments and pg_dump version-specific directives with random tokens
    |> Enum.reject(&String.starts_with?(&1, "--"))
    |> Enum.reject(&String.starts_with?(&1, "\\restrict "))
    |> Enum.reject(&String.starts_with?(&1, "\\unrestrict "))
    |> Enum.reject(fn
      "0\t17\t0\tNOW" -> true
      "1\t1\t0\tNOW" -> true
      "1\t2\t0\tNOW" -> true
      "1\t3\t0\tNOW" -> true
      "1\t3\t2\tNOW" -> true
      _line -> false
    end)
  end

  defp create_new_database(config) do
    :ok = EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)

    {:ok, conn} = Postgrex.start_link(config)

    EventStore.Storage.Initializer.run!(conn, config)

    :ok = GenServer.stop(conn)
  end

  defp restore_pre_migration_event_store(config) do
    EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)

    {_, 0} = EventStore.Storage.Database.restore(config, "test/fixture/eventstore.dump")
  end

  defp migrate_eventstore(config, opts) do
    EventStore.Tasks.Migrate.exec(config, opts)
  end
end
