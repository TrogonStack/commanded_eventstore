defmodule EventStore.AdvisoryLocks.Protocol do
  @moduledoc false

  # Postgrex closes a connection by sending a cancel request first, and both its
  # connect and its wait for the reply use the :timeout option, so this can take
  # that long when the database stops answering.
  # The lock owners cannot wait that long to hear the session is gone, and the
  # lock server cannot handle that notice until the socket is closed.

  @behaviour DBConnection

  @session_listener {__MODULE__, :session_listener}
  @cancel_timeout 1_000

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
  def disconnect(error, %Postgrex.Protocol{timeout: timeout} = state) do
    notify_session_lost(error)

    Postgrex.Protocol.disconnect(error, %Postgrex.Protocol{
      state
      | timeout: cancel_timeout(timeout)
    })
  end

  defp cancel_timeout(:infinity), do: @cancel_timeout
  defp cancel_timeout(timeout), do: min(timeout, @cancel_timeout)

  defp notify_session_lost(_error) do
    case Process.get(@session_listener) do
      nil -> :ok
      listener -> send(listener, {EventStore.AdvisoryLocks, :session_lost, self()})
    end
  end

  # A retry runs on the next connection before the lock server has handled the
  # loss of this session, so the lock owners would hear of it only afterwards.
  defp without_retry({:disconnect_and_retry, error, state}), do: {:disconnect, error, state}
  defp without_retry(result), do: result

  @impl DBConnection
  defdelegate checkout(state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate ping(state), to: Postgrex.Protocol

  @impl DBConnection
  def handle_begin(opts, state),
    do: without_retry(Postgrex.Protocol.handle_begin(opts, state))

  @impl DBConnection
  defdelegate handle_commit(opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_rollback(opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_status(opts, state), to: Postgrex.Protocol

  @impl DBConnection
  def handle_prepare(query, opts, state),
    do: without_retry(Postgrex.Protocol.handle_prepare(query, opts, state))

  @impl DBConnection
  def handle_execute(query, params, opts, state),
    do: without_retry(Postgrex.Protocol.handle_execute(query, params, opts, state))

  @impl DBConnection
  def handle_close(query, opts, state),
    do: without_retry(Postgrex.Protocol.handle_close(query, opts, state))

  @impl DBConnection
  defdelegate handle_declare(query, params, opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_fetch(query, cursor, opts, state), to: Postgrex.Protocol

  @impl DBConnection
  defdelegate handle_deallocate(query, cursor, opts, state), to: Postgrex.Protocol
end
