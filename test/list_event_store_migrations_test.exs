defmodule EventStore.ListEventStoreMigrationsTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    [config: config]
  end

  describe "event store with pending migrations" do
    test "should list completed and pending migrations", %{config: config} do
      restore_pre_migration_event_store(config)

      expected = """

      EventStore: TestEventStore

        migration     state           migrated_at
      -------------------------------------------------------------
        0.17.0    \tcompleted\t2020-04-28 22:15:48.922963Z
        1.1.0     \tpending
        1.2.0     \tpending
        1.3.0     \tpending
        1.3.2     \tpending
        subscription_lock_generation\tpending

      """

      assert expected == list_migrations(config, eventstore: TestEventStore)
    end

    test "exits with status 1 when running via mix", %{config: config} do
      restore_pre_migration_event_store(config)

      assert {:shutdown, 1} ==
               catch_exit(
                 Mix.Task.run("event_store.migrations", ["-e", "TestEventStore", "--quiet"])
               )
    end
  end

  describe "recently created event store" do
    test "lists migrations as completed", %{config: config} do
      create_new_database(config)

      expected_header = """

      EventStore: TestEventStore

        migration     state           migrated_at
      -------------------------------------------------------------
      """

      output = list_migrations(config, eventstore: TestEventStore)

      assert String.starts_with?(output, expected_header)

      migrations =
        output |> String.split("\n") |> Enum.drop(5) |> Enum.filter(&(String.trim(&1) != ""))

      assert Enum.all?(migrations, &String.contains?(&1, "completed"))
      assert Enum.any?(migrations, &String.contains?(&1, "subscription_lock_generation"))
    end

    test "exits normally when running via mix", %{config: config} do
      create_new_database(config)

      # Task should exit normally
      Mix.Task.run("event_store.migrations", ["-e", "TestEventStore", "--quiet"])
    end
  end

  describe "event store that recorded upstream's 1.5.0" do
    test "lists the lock generation migration as pending", %{config: config} do
      create_new_database(config)

      {:ok, conn} = Postgrex.start_link(config)
      Postgrex.query!(conn, "DROP TABLE fork_schema_migrations", [])
      Postgrex.query!(conn, "ALTER TABLE subscriptions DROP COLUMN lock_generation", [])

      Postgrex.query!(
        conn,
        "INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 5, 0)",
        []
      )

      :ok = GenServer.stop(conn)

      output = list_migrations(config, eventstore: TestEventStore)

      assert output =~ ~r/1\.5\.0\s+\tcompleted/
      assert output =~ ~r/subscription_lock_generation\s*\tpending/
    end
  end

  describe "event store in its own schema beside a migrated one" do
    test "lists the migrations of its own schema", %{config: config} do
      restore_pre_migration_event_store(config)

      {:ok, conn} = Postgrex.start_link(config)
      Postgrex.query!(conn, ~s(ALTER SCHEMA public RENAME TO "example"), [])
      Postgrex.query!(conn, "CREATE SCHEMA public", [])
      EventStore.Storage.Initializer.run!(conn, config)
      :ok = GenServer.stop(conn)

      output =
        list_migrations(Keyword.put(config, :schema, "example"), eventstore: TestEventStore)

      assert output =~ ~r/0\.17\.0\s+\tcompleted/
      assert output =~ ~r/1\.3\.2\s+\tpending/
      assert output =~ ~r/subscription_lock_generation\s*\tpending/
    end
  end

  describe "event store in its own schema beside one without the lock generation" do
    test "lists the lock generation migration of its own schema as completed", %{config: config} do
      create_new_database(config)

      {:ok, conn} = Postgrex.start_link(config)
      Postgrex.query!(conn, ~s(ALTER SCHEMA public RENAME TO "example"), [])
      Postgrex.query!(conn, "CREATE SCHEMA public", [])
      EventStore.Storage.Initializer.run!(conn, config)
      Postgrex.query!(conn, "DELETE FROM public.fork_schema_migrations", [])
      :ok = GenServer.stop(conn)

      output =
        list_migrations(Keyword.put(config, :schema, "example"), eventstore: TestEventStore)

      assert output =~ ~r/subscription_lock_generation\s*\tcompleted/
    end
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

  defp list_migrations(config, opts) do
    capture_io(fn -> EventStore.Tasks.Migrations.exec(config, opts) end)
  end
end
