defmodule EventStore.Storage.Session do
  @moduledoc false

  # The id is a session level setting the session sets the first time it is read,
  # because pg_stat_activity hides backend_start from a role switched to with SET ROLE.

  alias EventStore.Sql.Statements

  @type t :: %__MODULE__{backend_pid: pos_integer(), id: String.t()}

  defstruct [:backend_pid, :id]

  def new(backend_pid, id) when is_integer(backend_pid) and is_binary(id) and id != "" do
    %__MODULE__{backend_pid: backend_pid, id: id}
  end

  def current(conn, opts) do
    case Postgrex.query(conn, Statements.query_session(), [], opts) do
      {:ok, %Postgrex.Result{rows: [[backend_pid, id]]}} ->
        {:ok, new(backend_pid, id)}

      {:error, _error} = reply ->
        reply
    end
  end
end
