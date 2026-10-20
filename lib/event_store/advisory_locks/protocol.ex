defmodule EventStore.AdvisoryLocks.Protocol do
  @moduledoc false

  # Postgrex closes a connection by sending a cancel request first, which can
  # take as long as the connection timeout when the database stops answering.
  # The lock owners cannot wait that long to hear the session is gone.

  @behaviour DBConnection

  @session_listener {__MODULE__, :session_listener}

  def start_link(opts) do
    opts = Postgrex.Utils.default_opts(opts)

    DBConnection.start_link(__MODULE__, opts)
  end

  @impl DBConnection
  def connect(opts) do
    Process.put(@session_listener, Keyword.get(opts, :session_listener))

    Postgrex.Protocol.connect(opts)
  end

  @impl DBConnection
  def disconnect(error, state) do
    notify_session_lost(error)

    Postgrex.Protocol.disconnect(error, state)
  end

  defp notify_session_lost(_error) do
    case Process.get(@session_listener) do
      nil -> :ok
      listener -> send(listener, {EventStore.AdvisoryLocks, :session_lost, self()})
    end
  end

  @impl DBConnection
  defdelegate checkout(state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate ping(state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_begin(opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_commit(opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_rollback(opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_status(opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_prepare(query, opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_execute(query, params, opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_close(query, opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_declare(query, params, opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_fetch(query, cursor, opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_deallocate(query, cursor, opts, state), to: Postgrex.Protocol
end
