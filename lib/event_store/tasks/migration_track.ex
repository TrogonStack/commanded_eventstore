defmodule EventStore.Tasks.MigrationTrack do
  @moduledoc false

  alias EventStore.Tasks.Migration

  # Upstream commanded/eventstore records its migrations in `schema_migrations` and
  # only runs versions above the highest one recorded there. Migrations added by
  # this fork are recorded in a table of their own, so a version used by one side
  # can never hide a migration of the other.

  @type t :: %__MODULE__{
          name: :upstream | :trogon,
          table: String.t(),
          directory: String.t(),
          versions: [String.t()]
        }

  defstruct [:name, :table, :directory, :versions]

  @doc "Every track, in the order their migrations run."
  @spec all() :: [t()]
  def all, do: [upstream(), trogon()]

  @spec upstream() :: t()
  def upstream do
    %__MODULE__{
      name: :upstream,
      table: "schema_migrations",
      directory: "priv/event_store/migrations",
      versions: ["0.13.0", "0.14.0", "0.17.0", "1.1.0", "1.2.0", "1.3.0", "1.3.2"]
    }
  end

  @spec trogon() :: t()
  def trogon do
    %__MODULE__{
      name: :trogon,
      table: "trogon_schema_migrations",
      directory: "priv/event_store/migrations/trogon",
      versions: ["1.5.0"]
    }
  end

  @spec label(t(), String.t()) :: String.t()
  def label(%__MODULE__{name: :upstream}, version), do: version
  def label(%__MODULE__{name: :trogon}, version), do: "trogon/" <> version

  @spec script_path(t(), String.t()) :: Path.t()
  def script_path(%__MODULE__{directory: directory}, version) do
    Application.app_dir(:eventstore, Path.join(directory, "v#{version}.sql"))
  end

  @doc "The migrations of a track that are newer than every one it recorded."
  @spec pending(t(), completed :: [Migration.t()]) :: [Migration.t()]
  def pending(%__MODULE__{} = track, completed) do
    %__MODULE__{versions: versions} = track

    versions
    |> Enum.filter(&newer_than_all?(&1, completed))
    |> Enum.map(&%Migration{track: track, version: &1})
  end

  defp newer_than_all?(version, completed) do
    Enum.all?(completed, fn %Migration{version: done} -> Version.compare(version, done) == :gt end)
  end

  @doc "The migrations a track recorded in a database, oldest first."
  @spec completed(DBConnection.conn(), schema :: String.t(), t()) :: [Migration.t()]
  def completed(conn, schema, %__MODULE__{name: :trogon} = track) do
    # Databases created by upstream, or before this fork had migrations, have no table yet.
    if table_exists?(conn, schema, track), do: query_completed(conn, schema, track), else: []
  end

  def completed(conn, schema, %__MODULE__{} = track), do: query_completed(conn, schema, track)

  defp table_exists?(conn, schema, %__MODULE__{table: table}) do
    %Postgrex.Result{rows: [[exists]]} =
      Postgrex.query!(conn, "SELECT to_regclass($1) IS NOT NULL", [~s("#{schema}"."#{table}")])

    exists
  end

  defp query_completed(conn, schema, %__MODULE__{table: table} = track) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
          SELECT major_version, minor_version, patch_version, migrated_at
          FROM "#{schema}"."#{table}"
          ORDER BY 1, 2, 3
        """,
        []
      )

    Enum.map(rows, fn [major_version, minor_version, patch_version, migrated_at] ->
      %Migration{
        track: track,
        version: "#{major_version}.#{minor_version}.#{patch_version}",
        state: :completed,
        migrated_at: migrated_at
      }
    end)
  end
end
