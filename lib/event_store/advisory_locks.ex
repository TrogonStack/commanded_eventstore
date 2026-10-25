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
      :event_store,
      :lost_connection,
      :monitor,
      :query_timeout,
      :ref,
      :schema,
      :verify_timer,
      locks: %{},
      released: [],
      reported_claim_problems: MapSet.new()
    ]

    def new(opts) do
      conn = Keyword.fetch!(opts, :conn)
      event_store = Keyword.fetch!(opts, :event_store)
      monitor = Keyword.fetch!(opts, :monitor)
      query_timeout = Keyword.fetch!(opts, :query_timeout)
      schema = Keyword.fetch!(opts, :schema)

      %State{
        conn: conn,
        event_store: event_store,
        monitor: monitor,
        query_timeout: query_timeout,
        schema: schema
      }
    end
  end

  defmodule ClaimProblem do
    @moduledoc false

    # A claim that fails on the schema or on the lock role's grants keeps failing
    # on every retry until an operator acts, so it is reported to them.

    @type t :: :schema_outdated | :missing_grants

    @spec from_error(Postgrex.Error.t()) :: {:ok, t()} | :error
    def from_error(%Postgrex.Error{postgres: %{code: :undefined_column}}),
      do: {:ok, :schema_outdated}

    def from_error(%Postgrex.Error{postgres: %{code: :insufficient_privilege}}),
      do: {:ok, :missing_grants}

    def from_error(%Postgrex.Error{}), do: :error

    @spec message(t(), event_store :: atom(), schema :: String.t(), Postgrex.Error.t()) ::
            String.t()
    def message(:schema_outdated, event_store, _schema, error) do
      "EventStore #{inspect(event_store)} cannot start persistent subscriptions because " <>
        "its database schema predates v1.5.0. Run `mix event_store.migrate` (or " <>
        "`EventStore.Tasks.Migrate.exec/2`); subscriptions keep retrying until then. " <>
        "Reason: " <> Exception.message(error)
    end

    def message(:missing_grants, event_store, schema, error) do
      "EventStore #{inspect(event_store)} cannot start persistent subscriptions because " <>
        "the role of its session mode connection lacks privileges on " <>
        ~s("#{schema}".subscriptions. Grant it SELECT and UPDATE on that table; ) <>
        "subscriptions keep retrying until then. Reason: " <> Exception.message(error)
    end
  end

  defmodule Lock do
    @moduledoc false

    alias EventStore.Storage.Lock.Lease, as: StorageLease
    alias EventStore.Storage.Session

    @type t :: %Lock{
            key: non_neg_integer(),
            owner: pid(),
            ref: reference(),
            session: Session.t(),
            lease: StorageLease.t() | nil
          }
    defstruct [:key, :owner, :ref, :session, :lease]

    def new(key, owner, %Session{} = session, lease \\ nil) do
      %Lock{key: key, owner: owner, ref: make_ref(), session: session, lease: lease}
    end

    def ref(%Lock{ref: ref}), do: ref

    def held_by?(%Lock{session: session}, %Session{} = current), do: session == current
  end

  defmodule Lease do
    @moduledoc false

    alias EventStore.Storage.Lock.Lease, as: StorageLease
    alias EventStore.Storage.Session
    alias EventStore.Subscriptions.FencingToken

    @type t :: %Lease{
            ref: reference(),
            token: FencingToken.t(),
            last_seen: non_neg_integer() | nil,
            session: Session.t()
          }
    defstruct [:ref, :token, :last_seen, :session]

    def new(ref, %StorageLease{} = lease) do
      %StorageLease{token: token, last_seen: last_seen, session: session} = lease

      %Lease{ref: ref, token: token, last_seen: last_seen, session: session}
    end
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
  alias EventStore.AdvisoryLocks.{ClaimProblem, Connection, Lease, Lock, State}
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
    # nobody, so it is replaced by one whose notices can be received. A pool that
    # is down has none, so the replacement is skipped instead of crashing startup.
    replace_unannounced_connection(conn)

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
  catch
    :exit, {:timeout, _call} -> {:error, :timeout}
  end

  @doc """
  Attempt to obtain the advisory lock of a subscription, recording the new lock
  owner on the subscription row.

  Returns `{:ok, lease}` when the lock is acquired, `{:error, :lock_already_taken}`
  if it cannot be acquired immediately, or `{:error, :subscription_not_found}`
  when the subscription no longer exists. The lock is released and lost the same
  way as the one returned by `try_advisory_lock/3`, identified by `lease.ref`.
  """
  @spec try_subscription_lock(server :: GenServer.server(), pos_integer(), timeout()) ::
          {:ok, Lease.t()}
          | {:error, :lock_already_taken}
          | {:error, :subscription_not_found}
          | {:error, term}
  def try_subscription_lock(server, subscription_id, timeout \\ 5_000)
      when is_integer(subscription_id) do
    GenServer.call(server, {:try_subscription_lock, subscription_id, self(), timeout}, timeout)
  catch
    :exit, {:timeout, _call} -> {:error, :timeout}
  end

  @doc """
  Release a lock acquired by `try_advisory_lock/3` or `try_subscription_lock/3`
  before its owner terminates.

  The owner is not sent a `lock_released` message. Releasing a reference that is
  no longer held succeeds.

  Returns as soon as the request is queued so a caller is never held up by a
  lock connection that has stopped answering.
  """
  @spec release(server :: GenServer.server(), reference()) :: :ok
  def release(server, ref) when is_reference(ref) do
    GenServer.cast(server, {:release, ref})
  end

  def handle_cast({:release, ref}, %State{} = state) do
    state = handle_pending_session_loss(state)
    %State{locks: locks} = state

    state =
      case Enum.find(locks, fn {_owner_ref, lock} -> Lock.ref(lock) == ref end) do
        nil ->
          state

        {owner_ref, %Lock{} = lock} ->
          Process.demonitor(owner_ref, [:flush])

          release_held_lock(lock, %State{state | locks: Map.delete(locks, owner_ref)})
      end

    {:noreply, state}
  end

  def handle_call(
        {:try_subscription_lock, subscription_id, owner, timeout},
        _from,
        %State{} = state
      ) do
    state = handle_pending_session_loss(state)

    case {held_lock(state, subscription_id, owner),
          claim_subscription_lock(subscription_id, timeout, state)} do
      {nil, {:ok, %Storage.Lock.Lease{session: session} = lease}} ->
        lock = Lock.new(subscription_id, owner, session, lease)
        state = monitor_acquired_lock(lock, state)

        {:reply, {:ok, Lease.new(Lock.ref(lock), lease)}, state}

      {{owner_ref, %Lock{} = held}, {:ok, %Storage.Lock.Lease{session: session} = lease}} ->
        %Lock{key: key, ref: ref} = held
        %State{locks: locks} = state

        if Lock.held_by?(held, session), do: release_lock(key, state)

        lock = %Lock{held | session: session, lease: lease}

        {:reply, {:ok, Lease.new(ref, lease)},
         %State{state | locks: Map.put(locks, owner_ref, lock)}}

      {_held, {:error, %Postgrex.Error{} = error} = reply} ->
        {:reply, reply, report_claim_problem(error, state)}

      {_held, reply} ->
        {:reply, reply, state}
    end
  end

  def handle_call({:try_advisory_lock, key, owner, timeout}, _from, %State{} = state) do
    state = handle_pending_session_loss(state)

    case held_lock(state, key, owner) do
      {_owner_ref, %Lock{} = lock} ->
        {:reply, {:ok, Lock.ref(lock)}, state}

      nil ->
        case try_acquire_exclusive_lock(key, owner, timeout, state) do
          {:ok, %Lock{} = lock} ->
            state = monitor_acquired_lock(lock, state)

            {:reply, {:ok, Lock.ref(lock)}, state}

          reply ->
            {:reply, reply, state}
        end
    end
  end

  defp claim_subscription_lock(subscription_id, timeout, %State{} = state) do
    %State{schema: schema} = state

    query(state, fn conn ->
      Storage.Lock.claim_subscription_lock(conn, subscription_id,
        schema: schema,
        timeout: timeout
      )
    end)
  end

  defp report_claim_problem(%Postgrex.Error{} = error, %State{} = state) do
    %State{event_store: event_store, schema: schema, reported_claim_problems: reported} = state

    with {:ok, problem} <- ClaimProblem.from_error(error),
         false <- MapSet.member?(reported, problem) do
      Logger.error(ClaimProblem.message(problem, event_store, schema, error))

      %State{state | reported_claim_problems: MapSet.put(reported, problem)}
    else
      _reported_or_transient -> state
    end
  end

  defp held_lock(%State{locks: locks}, key, owner) do
    Enum.find(locks, fn
      {_owner_ref, %Lock{key: ^key, owner: ^owner}} -> true
      _other -> false
    end)
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

    state = cancel_verify_retry(state)

    {:noreply, %State{state | connection: nil, locks: %{}, released: []}}
  end

  # A session that could not be verified never reported a disconnect, so the
  # pending unlocks are reconciled again once it can answer.
  def handle_info({:timeout, ref, :verify_sessions}, %State{verify_timer: ref} = state),
    do: {:noreply, verify_sessions(%State{state | verify_timer: nil})}

  # A retry already cancelled or replaced would otherwise poll alongside the pending one.
  def handle_info({:timeout, _ref, :verify_sessions}, %State{} = state), do: {:noreply, state}

  # The connection pool outlives its connections, so a reconnect replaces the
  # session that held the locks without the pool going down.
  def handle_info({:connected, conn_pid}, %State{} = state) when is_pid(conn_pid),
    do: {:noreply, connected(conn_pid, state)}

  def handle_info({:connected, conn_pid, _tag}, %State{} = state) when is_pid(conn_pid),
    do: {:noreply, connected(conn_pid, state)}

  # Sent as the connection starts closing, which can take a long time when the
  # database stops answering, ahead of its `:disconnected` notice.
  def handle_info({__MODULE__, :session_lost, conn_pid}, %State{} = state) when is_pid(conn_pid),
    do: {:noreply, disconnected(conn_pid, :session_ended, state)}

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
    state = handle_pending_session_loss(state)
    %State{locks: locks} = state

    state =
      case Map.get(locks, ref) do
        nil ->
          state

        %Lock{} = lock ->
          release_held_lock(lock, %State{state | locks: Map.delete(locks, ref)})
      end

    {:noreply, state}
  end

  def handle_info({:EXIT, _, :normal}, state) do
    {:noreply, state}
  end

  def handle_info({:EXIT, _from, reason}, state) do
    {:stop, reason, state}
  end

  # A query held up by a connection that stopped answering leaves the notice of
  # its lost session queued behind the requests that arrived meanwhile, so it is
  # handled before them instead of after each one has waited on the pool.
  defp handle_pending_session_loss(%State{connection: %Connection{pid: conn_pid}} = state) do
    receive do
      {__MODULE__, :session_lost, ^conn_pid} -> disconnected(conn_pid, :session_ended, state)
    after
      0 -> state
    end
  end

  defp handle_pending_session_loss(%State{} = state), do: state

  defp connected(conn_pid, %State{connection: connection} = state) do
    Connection.demonitor(connection)

    verify_sessions(%State{
      state
      | connection: Connection.monitor(conn_pid),
        lost_connection: nil
    })
  end

  defp disconnected(conn_pid, _reason, %State{lost_connection: conn_pid} = state), do: state

  # A lock taken through the next connection can be recorded before this notice
  # is handled, so it is released here too and only unlocked once verified.
  defp disconnected(conn_pid, reason, %State{connection: connection} = state) do
    if Connection.current?(connection, conn_pid) do
      %State{locks: locks, released: released} = state

      Connection.demonitor(connection)

      notify_lost_locks(locks, reason)

      %State{
        state
        | connection: nil,
          lost_connection: conn_pid,
          locks: %{},
          released: Map.values(locks) ++ released
      }
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

        # A live session confirms the locks still held, so polling stops once
        # the pending unlocks are drained.
        maybe_schedule_verify_retry(%State{state | locks: held, released: pending})

      # A failed query does not prove the session ended, so verification is
      # retried until a live session can confirm the locks and release them.
      {:error, _error} ->
        schedule_verify_retry(state)
    end
  end

  defp maybe_schedule_verify_retry(%State{released: []} = state), do: state
  defp maybe_schedule_verify_retry(%State{} = state), do: schedule_verify_retry(state)

  defp schedule_verify_retry(%State{verify_timer: ref} = state) when is_reference(ref), do: state

  defp schedule_verify_retry(%State{} = state),
    do: %State{state | verify_timer: :erlang.start_timer(@verify_retry, self(), :verify_sessions)}

  defp cancel_verify_retry(%State{verify_timer: nil} = state), do: state

  defp cancel_verify_retry(%State{verify_timer: ref} = state) do
    :erlang.cancel_timer(ref)

    %State{state | verify_timer: nil}
  end

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

  # An owner holds a key once, so releasing it unlocks that owner's hold and leaves
  # the holds of every other owner in place.
  defp release_held_lock(%Lock{} = lock, %State{released: released} = state) do
    verify_sessions(%State{state | released: [lock | released]})
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

  defp replace_unannounced_connection(conn) do
    DBConnection.disconnect_all(conn, 0)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
