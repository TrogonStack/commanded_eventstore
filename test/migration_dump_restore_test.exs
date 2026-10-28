defmodule EventStore.MigrationDumpRestoreTest do
  use ExUnit.Case

  alias EventStore.Config
  alias EventStore.StorageCase

  @moduletag :migration

  test "restores the dump while another session is connected to the database" do
    config = Config.parsed(TestEventStore, :eventstore)
    postgrex_config = Config.default_postgrex_opts(config)

    EventStore.Storage.Database.create(config)
    conn = start_supervised!({Postgrex, postgrex_config})
    Postgrex.query!(conn, "SELECT 1", [])

    assert {_, 0} = StorageCase.restore_migration_database_dump(config)
  end
end
