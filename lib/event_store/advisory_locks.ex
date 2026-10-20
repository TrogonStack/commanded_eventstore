defmodule EventStore.AdvisoryLocks do
  @moduledoc false

  # PostgreSQL provides a means for creating locks that have application-defined
  # meanings. Advisory locks are faster, avoid table bloat, and are
  # automatically cleaned up by the server at the end of the session.

  use GenServer

  require Logger

  alias EventStore.MonitoredServer

  @verify_retry 1_000

  defmodule State do
    @moduledoc false
    defstruct [
      :conn,
      :connection,
      :monitor,
      :query_timeout,
      :ref,
      :schema,
      :verify_timer,
      locks: %{},
      released: []
    ]

    def new(opts) do
      conn = Keyword.fetch!(opts, :conn)
      monitor = Keyword.fetch!(opts, :monitor)
      query_timeout = Keyword.fetch!(opts, :query_timeout)
      schema = Keyword.fetch!(opts, :schema)

      %State{conn: conn, monitor: monitor, query_timeout: query_timeout, schema: schema}
    end
  end

  defmodule Lock do
    @moduledoc false

    alias EventStore.Storage.Session

    @type t :: %Lock{
            key: non_neg_integer(),
            owner: pid(),
            ref: reference(),
            session: Session.t()
          }
    defstruct [:key, :owner, :ref, :session]

    def new(key, owner, %Session{} = session) do
      %Lock{key: key, owner: owner, ref: make_ref(), session: session}
    end

    def ref(%Lock{ref: ref}), do: ref

    def held_by?(%Lock{session: session}, %Session{} = current), do: session == current
  end

  defmodule Connection do
    @moduledoc false

    @type t :: %Connection{pid: pid(), monitor_ref: reference()}
    defstruct [:pid, :monitor_ref]

    def monitor(pid), do: %Connection{pid: pid, monitor_ref: Process.monitor(pid)}

    def demonitor(nil), do: :ok

    def demonitor(%Connection{monitor_ref: monitor_ref}),
      do: Process.demonitor(monitor_ref, [:flush])

    # Without a known connection any notice may be about the one holding the locks.
    def current?(nil, _pid), do: true
    def current?(%Connection{pid: pid}, pid), do: true
    def current?(%Connection{}, _pid), do: false
  end

  alias DBConnection.ConnectionError
  alias EventStore.AdvisoryLocks.{Connection, Lock, State}
  alias EventStore.Storage
  alias EventStore.Storage.Session

  def start_link(opts) do
    {start_opts, advisory_locks_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    state = State.new(advisory_locks_opts)

    GenServer.start_link(__MODULE__, state, start_opts)
  end

  def init(%State{} = state) do
    Process.flag(:trap_exit, true)

    %State{conn: conn, monitor: monitor} = state

    {:ok, ref} = MonitoredServer.monitor(monitor)

    # A connection made before this server was registered announced itself to
    # nobody, so it is replaced by one whose notices can be received.
    :ok = DBConnection.disconnect_all(conn, 0)

    {:ok, %State{state | ref: ref}}
  end

  @doc """
  Attempt to obtain an advisory lock.

     - `key` - an application specific integer to acquire a lock on.
     - `timeout` - an integer greater than zero which specifies how many
        milliseconds to wait for a reply, or the atom `:infinity` to wait
        indefinitely.

  Returns `{:ok, lock}` when lock successfully acquired, or
  `{:error, :lock_already_taken}` if the lock cannot be acquired immediately.

  ## Lock released

  An `{EventStore.AdvisoryLocks, :lock_released, lock, reason}` message will be
  sent to the lock owner when the lock is released, usually due to the database
  connection terminating. It is up to the lock owner to attempt to reacquire the
  lost lock.

  """
  @spec try_advisory_lock(server :: GenServer.server(), key :: non_neg_integer(), timeout()) ::
          {:ok, reference()} | {:error, :lock_already_taken} | {:error, term}
  def try_advisory_lock(server, key, timeout \\ 5_000) when is_integer(key) do
    GenServer.call(server, {:try_advisory_lock, key, self(), timeout}, timeout)
  end

  def handle_call({:try_advisory_lock, key, owner, timeout}, _from, %State{} = state) do
    case try_acquire_exclusive_lock(key, owner, timeout, state) do
      {:ok, %Lock{} = lock} ->
        state = monitor_acquired_lock(lock, state)

        {:reply, {:ok, Lock.ref(lock)}, state}

      reply ->
        {:reply, reply, state}
    end
  end

  defp try_acquire_exclusive_lock(key, owner, timeout, %State{} = state) do
    %State{schema: schema} = state

    with {:ok, %Session{} = session} <-
           query(state, fn conn ->
             Storage.Lock.try_acquire_exclusive_session_lock(conn, key,
               schema: schema,
               timeout: timeout
             )
           end) do
      {:ok, Lock.new(key, owner, session)}
    end
  end

  defp monitor_acquired_lock(%Lock{} = lock, %State{locks: locks} = state) do
    %Lock{owner: owner} = lock

    owner_ref = Process.monitor(owner)

    %State{state | locks: Map.put(locks, owner_ref, lock)}
  end

  # Lost locks when database connection goes down.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{ref: ref} = state) do
    %State{locks: locks} = state

    notify_lost_locks(locks, reason)

    {:noreply, %State{state | connection: nil, locks: %{}, released: [], verify_timer: nil}}
  end

  # A session that could not be verified never reported a disconnect, so the
  # pending unlocks are reconciled again once it can answer.
  def handle_info(:verify_sessions, %State{} = state),
    do: {:noreply, verify_sessions(%State{state | verify_timer: nil})}

  # The connection pool outlives its connections, so a reconnect replaces the
  # session that held the locks without the pool going down.
  def handle_info({:connected, conn_pid}, %State{} = state) when is_pid(conn_pid),
    do: {:noreply, connected(conn_pid, state)}

  def handle_info({:connected, conn_pid, _tag}, %State{} = state) when is_pid(conn_pid),
    do: {:noreply, connected(conn_pid, state)}

  def handle_info({:disconnected, conn_pid}, %State{} = state) when is_pid(conn_pid),
    do: {:noreply, disconnected(conn_pid, :session_ended, state)}

  def handle_info({:disconnected, conn_pid, _tag}, %State{} = state) when is_pid(conn_pid),
    do: {:noreply, disconnected(conn_pid, :session_ended, state)}

  def handle_info(
        {:DOWN, monitor_ref, :process, conn_pid, reason},
        %State{connection: %Connection{monitor_ref: monitor_ref}} = state
      ),
      do: {:noreply, disconnected(conn_pid, reason, state)}

  # Release lock when the lock owner process terminates.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    %State{locks: locks} = state

    state =
      case Map.get(locks, ref) do
        nil ->
          state

        %Lock{key: key} ->
          release_lock(key, state)

          %State{state | locks: Map.delete(locks, ref)}
      end

    {:noreply, state}
  end

  def handle_info({:EXIT, _, :normal}, state) do
    {:noreply, state}
  end

  def handle_info({:EXIT, _from, reason}, state) do
    {:stop, reason, state}
  end

  defp connected(conn_pid, %State{connection: connection} = state) do
    Connection.demonitor(connection)

    verify_sessions(%State{state | connection: Connection.monitor(conn_pid)})
  end

  # A lock taken through the next connection can be recorded before this notice
  # is handled, so it is released here too and only unlocked once verified.
  defp disconnected(conn_pid, reason, %State{connection: connection} = state) do
    if Connection.current?(connection, conn_pid) do
      %State{locks: locks, released: released} = state

      Connection.demonitor(connection)

      notify_lost_locks(locks, reason)

      %State{state | connection: nil, locks: %{}, released: Map.values(locks) ++ released}
    else
      state
    end
  end

  defp verify_sessions(%State{locks: locks, released: []} = state) when map_size(locks) == 0,
    do: state

  defp verify_sessions(%State{} = state) do
    %State{locks: locks, query_timeout: query_timeout, released: released} = state

    case query(state, &Session.current(&1, timeout: query_timeout)) do
      {:ok, %Session{} = session} ->
        {held, lost} = Map.split_with(locks, fn {_ref, lock} -> Lock.held_by?(lock, session) end)

        notify_lost_locks(lost, :session_ended)

        # A lock whose session has ended died with it, but one still held by this
        # session is only cleared once its unlock is acknowledged.
        pending =
          for %Lock{key: key} = lock <- released,
              Lock.held_by?(lock, session),
              release_lock(key, state) != :ok,
              do: lock

        schedule_verify_retry(%State{state | locks: held, released: pending})

      # A failed query does not prove the session ended, so the pending unlocks
      # are retried until a live session can confirm and release them.
      {:error, _error} ->
        schedule_verify_retry(state)
    end
  end

  defp schedule_verify_retry(%State{released: []} = state), do: state
  defp schedule_verify_retry(%State{verify_timer: ref} = state) when is_reference(ref), do: state

  defp schedule_verify_retry(%State{} = state),
    do: %State{state | verify_timer: Process.send_after(self(), :verify_sessions, @verify_retry)}

  defp notify_lost_locks(locks, _reason) when map_size(locks) == 0, do: :ok

  defp notify_lost_locks(locks, reason) do
    Logger.warning(
      "Lost #{map_size(locks)} advisory lock(s) held by this event store due to: " <>
        inspect(reason)
    )

    for {owner_ref, %Lock{} = lock} <- locks do
      %Lock{owner: owner, ref: ref} = lock

      Process.demonitor(owner_ref, [:flush])

      :ok = Process.send(owner, {__MODULE__, :lock_released, ref, reason}, [])
    end

    :ok
  end

  defp release_lock(key, %State{} = state) do
    %State{query_timeout: query_timeout, schema: schema} = state

    query(state, &Storage.Lock.unlock(&1, key, schema: schema, timeout: query_timeout))
  end

  defp query(%State{conn: conn}, fun) do
    fun.(conn)
  catch
    :exit, reason ->
      {:error,
       ConnectionError.exception("advisory lock connection pool exited: " <> inspect(reason))}
  end
end
