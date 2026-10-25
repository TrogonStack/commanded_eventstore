defmodule EventStore.Tasks.Migration do
  @moduledoc false

  alias EventStore.Tasks.MigrationTrack

  @type t :: %__MODULE__{
          track: MigrationTrack.t(),
          version: String.t(),
          state: :pending | :completed,
          migrated_at: DateTime.t() | nil
        }

  defstruct track: nil, version: nil, state: :pending, migrated_at: nil
end

defmodule EventStore.Tasks.Migrations do
  @moduledoc """
  Task to show the migration status of EventStore
  """

  import EventStore.Tasks.Output

  alias EventStore.Config
  alias EventStore.Tasks.{Migration, MigrationTrack}

  @dialyzer {:no_return, exec: 2}

  @doc """
  Run task

  ## Parameters

    - config: the parsed EventStore config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)
    config = Config.default_postgrex_opts(config)

    migrations = migrations(config, schema)
    event_store = Keyword.get(opts, :eventstore, "default")
    event_store_name = event_store |> to_string() |> String.replace_prefix("Elixir.", "")

    write_info("\nEventStore: #{event_store_name}\n", opts)
    write_table_header(opts)

    Enum.each(migrations, &list_migration(&1, opts))

    write_info("", opts)

    Enum.map(migrations, & &1.state)
  end

  defp list_migration(%Migration{state: :pending} = migration, opts) do
    write_info("  #{label(migration)}\tpending", opts)
  end

  defp list_migration(%Migration{state: :completed, migrated_at: time} = migration, opts) do
    write_info("  #{label(migration)}\tcompleted\t#{time}", opts)
  end

  defp label(%Migration{track: track, version: version}) do
    track |> MigrationTrack.label(version) |> String.pad_trailing(10)
  end

  defp write_table_header(opts) do
    """
      migration     state           migrated_at
    -------------------------------------------------------------
    """
    |> String.trim_trailing()
    |> write_info(opts)
  end

  defp migrations(config, schema) do
    {:ok, conn} = Postgrex.start_link(config)

    try do
      Enum.flat_map(MigrationTrack.all(), fn track ->
        completed = MigrationTrack.completed(conn, schema, track)

        completed ++ MigrationTrack.pending(track, completed)
      end)
    after
      GenServer.stop(conn)
    end
  end
end
