defmodule EventStore.Storage.Session do
  @moduledoc false

  # A backend process ID alone may be reused by a later session, for example
  # after the database server restarts, so the start time is part of the identity.

  alias EventStore.Sql.Statements

  @type t :: %__MODULE__{backend_pid: pos_integer(), started_at: DateTime.t()}

  defstruct [:backend_pid, :started_at]

  def new(backend_pid, %DateTime{} = started_at) when is_integer(backend_pid) do
    %__MODULE__{backend_pid: backend_pid, started_at: started_at}
  end

  def current(conn, opts) do
    case Postgrex.query(conn, Statements.query_session(), [], opts) do
      {:ok, %Postgrex.Result{rows: [[backend_pid, started_at]]}} ->
        {:ok, new(backend_pid, started_at)}

      {:error, _error} = reply ->
        reply
    end
  end
end
