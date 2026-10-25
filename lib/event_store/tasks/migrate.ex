defmodule EventStore.Tasks.Migrate do
  @moduledoc """
  Task to migrate EventStore
  """

  import EventStore.Tasks.Output

  alias EventStore.{Config, Storage}
  alias EventStore.Tasks.{Migration, MigrationTrack}

  @dialyzer {:no_return, exec: 2}

  @doc """
  Run task

  ## Parameters

    - config: the parsed EventStore config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)
    config = Config.default_postgrex_opts(config)

    with_migration_lock(config, schema, opts, fn ->
      {:ok, conn} = Postgrex.start_link(config)

      try do
        migrations = pending_migrations(conn, schema)

        migrate(conn, schema, opts, migrations)
      after
        GenServer.stop(conn)
      end
    end)
  end

  # Prevent database migrations from running concurrently by acquiring a
  # Postgres advisory lock.
  defp with_migration_lock(config, schema, opts, callback) do
    {:ok, lock_conn} = Postgrex.start_link(config)

    try do
      with :ok <- acquire_migration_lock(lock_conn, schema) do
        callback.()
      else
        {:error, :lock_already_taken} ->
          write_info("EventStore database migration already in progress.", opts)

        {:error, %Postgrex.Error{} = error} ->
          raise_msg(
            "The EventStore database could not be migrated. Could not acquire migration lock due to: " <>
              inspect(error),
            opts
          )
      end
    after
      GenServer.stop(lock_conn)
    end
  end

  defp acquire_migration_lock(conn, schema) do
    Storage.Lock.try_acquire_exclusive_lock(conn, -1, schema: schema)
  end

  defp pending_migrations(conn, schema) do
    Enum.flat_map(MigrationTrack.all(), fn track ->
      MigrationTrack.pending(track, MigrationTrack.completed(conn, schema, track))
    end)
  end

  defp migrate(_conn, _schema, opts, []) do
    write_info("The EventStore database is already migrated.", opts)
  end

  defp migrate(conn, schema, opts, migrations) do
    for %Migration{track: track, version: version} <- migrations do
      write_info("Running migration #{MigrationTrack.label(track, "v" <> version)}...", opts)

      script = track |> MigrationTrack.script_path(version) |> File.read!()

      statements = [
        ~s(SET LOCAL search_path TO "#{schema}";),
        script
      ]

      case transaction(conn, statements) do
        {:ok, :ok} ->
          :ok

        {:error, error} ->
          raise_msg(
            "The EventStore database couldn't be migrated, reason given: " <>
              inspect(Exception.message(error)),
            opts
          )
      end
    end

    write_info("The EventStore database has been migrated.", opts)
  end

  defp transaction(conn, statements) do
    opts = [timeout: :infinity]

    Postgrex.transaction(
      conn,
      fn transaction ->
        Enum.each(statements, &Postgrex.query!(transaction, &1, [], opts))
      end,
      opts
    )
  end
end
