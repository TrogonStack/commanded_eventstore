defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  import ExUnit.CaptureLog

  alias EventStore.{AdvisoryLocks, Config, MonitoredServer, Wait}
  alias EventStore.Storage

  @locks TestEventStore.EventStore.AdvisoryLocks
  @conn TestEventStore.EventStore.AdvisoryLocks.Postgrex
  @monitor TestEventStore.EventStore.AdvisoryLocks.Postgrex.EventStore.MonitoredServer

  setup do
    conn = start_supervised!({Postgrex, postgrex_config()})

    [conn: conn]
  end

  describe "acquire lock" do
    test "should acquire lock when available" do
      assert {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert is_reference(lock)
    end

    test "should hand the owner the lock it already holds and free it with one release",
         %{conn: conn, schema: schema} do
      assert {:ok, lock1} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock2} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock3} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert is_reference(lock1)
      assert lock2 == lock1
      assert lock3 == lock1

      assert :ok = AdvisoryLocks.release(@locks, lock1)

      _state = :sys.get_state(@locks)

      assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      assert :ok = Storage.Lock.unlock(conn, 1, schema: schema)
    end

    test "should fail to acquire lock when already taken", %{conn: conn, schema: schema} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      assert {:error, :lock_already_taken} = AdvisoryLocks.try_advisory_lock(@locks, 1)
    end
  end

  describe "acquire subscription lock" do
    test "should acquire the lock and record the lock owner", context do
      %{subscription_id: subscription_id} = create_subscription(context)

      assert {:ok, %AdvisoryLocks.Lease{} = lease} =
               AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      assert is_reference(lease.ref)
      assert lease.token == EventStore.Subscriptions.FencingToken.new(subscription_id, 1)
      assert lease.last_seen == nil
    end

    test "should record a new lock owner each time the lock is acquired", context do
      %{subscription_id: subscription_id} = create_subscription(context)

      assert {:ok, %{token: %{generation: 1}}} =
               AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      assert {:ok, %{token: %{generation: 2}}} =
               AdvisoryLocks.try_subscription_lock(@locks, subscription_id)
    end

    test "should keep one hold when the owner claims the lock again", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      assert {:ok, %AdvisoryLocks.Lease{} = first} =
               AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      Postgrex.query!(
        conn,
        "UPDATE \"#{schema}\".subscriptions SET last_seen = 7 WHERE subscription_id = $1",
        [subscription_id]
      )

      assert {:ok, %AdvisoryLocks.Lease{} = second} =
               AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      assert second.ref == first.ref
      assert second.token.generation == first.token.generation + 1
      assert second.last_seen == 7

      assert :ok = AdvisoryLocks.release(@locks, first.ref)

      _state = :sys.get_state(@locks)

      assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: schema)
      assert :ok = Storage.Lock.unlock(conn, subscription_id, schema: schema)
    end

    test "should free the lock with one release after a claim outlived its caller", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, busy_ref} = AdvisoryLocks.try_advisory_lock(@locks, 2)

      holder = hold_only_connection()

      try do
        :ok = AdvisoryLocks.release(@locks, busy_ref)

        wait_until_waiting_for_connection()

        assert {:error, :timeout} =
                 AdvisoryLocks.try_subscription_lock(@locks, subscription_id, 300)
      after
        send(holder, :release)
      end

      _state = :sys.get_state(@locks, 5_000)

      assert {:ok, %AdvisoryLocks.Lease{ref: ref}} =
               AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      assert :ok = AdvisoryLocks.release(@locks, ref)

      _state = :sys.get_state(@locks, 5_000)

      assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: schema)
      assert :ok = Storage.Lock.unlock(conn, subscription_id, schema: schema)
    end

    test "should fail to acquire the lock when another session holds it", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      assert {:ok, _lease} =
               Storage.Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

      assert {:error, :lock_already_taken} =
               AdvisoryLocks.try_subscription_lock(@locks, subscription_id)
    end

    test "should keep every lock on the node while a table lock outlasts a claim", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: held_id} = create_subscription(context)

      {:ok, %AdvisoryLocks.Lease{ref: ref}} = AdvisoryLocks.try_subscription_lock(@locks, held_id)

      other = create_subscription(context, "other_subscription")

      table_lock = lock_subscriptions_table(context)

      try do
        assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
                 AdvisoryLocks.try_subscription_lock(@locks, other.subscription_id, 1_000)
      after
        release_table_lock(table_lock)
      end

      _state = :sys.get_state(@locks)

      refute_received {AdvisoryLocks, :lock_released, ^ref, _reason}

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, held_id, schema: schema)
    end

    test "should keep the lock held when a repeated claim cannot wait for a table lock",
         context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, %AdvisoryLocks.Lease{}} = AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      table_lock = lock_subscriptions_table(context)

      try do
        assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
                 AdvisoryLocks.try_subscription_lock(@locks, subscription_id, 1_000)
      after
        release_table_lock(table_lock)
      end

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: schema)
    end

    test "should keep the lock held when a repeated claim is cancelled while it waits", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, %AdvisoryLocks.Lease{}} = AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      lock_backend_pid = backend_pid(@conn)

      canceller = start_supervised!({Postgrex, postgrex_config()}, id: :canceller)

      table_lock = lock_subscriptions_table(context)

      try do
        cancel =
          Task.async(fn ->
            Wait.until(fn ->
              assert %Postgrex.Result{rows: [["Lock"]]} =
                       Postgrex.query!(
                         canceller,
                         "SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1",
                         [lock_backend_pid]
                       )
            end)

            Postgrex.query!(canceller, "SELECT pg_cancel_backend($1)", [lock_backend_pid])
          end)

        assert {:error, %Postgrex.Error{postgres: %{code: :query_canceled}}} =
                 AdvisoryLocks.try_subscription_lock(@locks, subscription_id, 10_000)

        assert %Postgrex.Result{rows: [[true]]} = Task.await(cancel)
      after
        release_table_lock(table_lock)
      end

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: schema)
    end

    test "should fail to acquire the lock of a subscription that does not exist" do
      assert {:error, :subscription_not_found} =
               AdvisoryLocks.try_subscription_lock(@locks, 999_999)
    end

    test "should send `lock_released` message with the lease reference", context do
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, %{ref: ref}} = AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      shutdown_database_connection()

      assert_receive({AdvisoryLocks, :lock_released, ^ref, :session_ended})
    end

    test "should release the lock when the owner terminates", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)
      reply_to = self()

      pid =
        spawn_link(fn ->
          {:ok, _lease} = AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

          send(reply_to, :lock_acquired)

          receive do
            :shutdown -> :ok
          end
        end)

      assert_receive :lock_acquired

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: schema)

      send(pid, :shutdown)

      Wait.until(fn ->
        assert :ok =
                 Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: schema)
      end)
    end
  end

  describe "release a lock by reference" do
    test "should free the key for another session", %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      assert :ok = AdvisoryLocks.release(@locks, lock)

      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should free a subscription lock for another session", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, %AdvisoryLocks.Lease{ref: ref}} =
        AdvisoryLocks.try_subscription_lock(@locks, subscription_id)

      assert {:error, :lock_already_taken} =
               Storage.Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

      assert :ok = AdvisoryLocks.release(@locks, ref)

      _ = :sys.get_state(@locks)

      Wait.until(fn ->
        assert {:ok, %{token: %{generation: 2}}} =
                 Storage.Lock.claim_subscription_lock(conn, subscription_id, schema: schema)
      end)
    end

    test "should keep the key held for another owner of the same key",
         %{conn: conn, schema: schema} do
      reply_to = self()

      other_owner =
        spawn_link(fn ->
          {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

          send(reply_to, :lock_acquired)

          receive do
            :shutdown -> :ok
          end
        end)

      assert_receive :lock_acquired

      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert :ok = AdvisoryLocks.release(@locks, lock)

      _state = :sys.get_state(@locks)

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      send(other_owner, :shutdown)

      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should ignore a reference that is not held" do
      assert :ok = AdvisoryLocks.release(@locks, make_ref())
    end

    test "should not notify the owner that the lock was released" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert :ok = AdvisoryLocks.release(@locks, lock)

      refute_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 200
    end

    test "should not wait for the lock server to answer", %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      :ok = :sys.suspend(@locks)

      try do
        {elapsed, :ok} = :timer.tc(fn -> AdvisoryLocks.release(@locks, lock) end, :millisecond)

        assert elapsed < 1_000
      after
        :ok = :sys.resume(@locks)
      end

      _state = :sys.get_state(@locks)

      assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
    end

    test "should not crash a claim the busy lock server cannot answer in time", context do
      %{subscription_id: held_id} = create_subscription(context)

      {:ok, %AdvisoryLocks.Lease{ref: ref}} = AdvisoryLocks.try_subscription_lock(@locks, held_id)

      other = create_subscription(context, "other_subscription")

      holder = hold_only_connection()

      try do
        :ok = AdvisoryLocks.release(@locks, ref)

        wait_until_waiting_for_connection()

        assert {:error, :timeout} =
                 AdvisoryLocks.try_subscription_lock(@locks, other.subscription_id, 300)
      after
        send(holder, :release)
      end

      _state = :sys.get_state(@locks, 5_000)
    end
  end

  describe "release lock" do
    test "should release lock when process terminates", %{conn: conn, schema: schema} do
      reply_to = self()

      pid =
        spawn_link(fn ->
          {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

          send(reply_to, :lock_acquired)

          # Wait until shutdown
          receive do
            :shutdown -> :ok
          end
        end)

      assert_receive :lock_acquired

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      send(pid, :shutdown)

      # Wait for lock to be released after process terminates
      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end
  end

  describe "disconnect" do
    test "should send `lock_released` message" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      shutdown_database_connection()

      assert_receive({AdvisoryLocks, :lock_released, ^lock, _reason})
    end

    test "should send `lock_released` message when the lock session ends and the connection reconnects",
         %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      terminate_lock_session(conn)

      assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 5_000

      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should keep a lock acquired after the connection reconnects", %{conn: conn} do
      {:ok, lost_lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      terminate_lock_session(conn)

      assert_receive {AdvisoryLocks, :lock_released, ^lost_lock, _reason}, 5_000

      kept_lock =
        Wait.until(fn ->
          assert {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 2)

          lock
        end)

      refute_receive {AdvisoryLocks, :lock_released, ^kept_lock, _reason}, 1_000
    end

    test "should send `lock_released` message without waiting for the connection to reconnect",
         %{conn: conn} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      without_reconnects(fn _lock_connection ->
        terminate_lock_session(conn)

        # Using the ended session makes the connection notice it now, not at its next idle ping.
        # The query bypasses the lock server so it stays free to handle the notice, and
        # `queue: false` stops a retry from waiting for a connection that cannot start.
        assert {:error, _error} = Postgrex.query(@conn, "SELECT 1", [], queue: false)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 500
      end)
    end

    test "should keep locks when a notice for a previous connection arrives late" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      send(@locks, {:connected, lock_connection_pid()})

      previous_connection = spawn(fn -> :ok end)

      send(@locks, {:disconnected, previous_connection})

      refute_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 500
    end

    test "should send `lock_released` message when the database stops answering new connections",
         %{conn: conn, schema: schema} do
      {locks, proxy} = start_proxied_lock_store(:proxied_lock_store, schema, idle_interval: 100)
      lock_pool = Module.concat([locks, Postgrex])

      try do
        {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks, 1)

        %Postgrex.Result{rows: [[backend_pid]]} =
          Postgrex.query!(lock_pool, "SELECT pg_backend_pid()", [])

        :ok = EventStore.TcpProxy.blackhole(proxy)

        %Postgrex.Result{rows: [[true]]} =
          Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

        Wait.until(fn ->
          assert %Postgrex.Result{rows: [[0]]} =
                   Postgrex.query!(
                     conn,
                     "SELECT count(*)::int FROM pg_stat_activity WHERE pid = $1",
                     [backend_pid]
                   )
        end)

        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 1_000
      after
        stop_supervised!(EventStore.TcpProxy)
      end
    end

    test "should keep a lock acquired while the lost connection is still closing and unlock it when its owner exits",
         %{conn: conn, schema: schema} do
      lock_connection = lock_connection_pid()

      send(@locks, {AdvisoryLocks, :session_lost, lock_connection})

      owner = start_lock_owner(1)

      assert_receive {:owner_acquired, ^owner, {:ok, lock}}, 1_000

      send(@locks, {:disconnected, lock_connection})

      # The lock server has handled the notice once it answers, and the owner forwards
      # its messages in order, so the probe arrives after any notification the notice sent.
      :sys.get_state(@locks)
      send(owner, :probe)
      assert_receive {:owner_notified, ^owner, :probe}, 1_000

      refute_received {:owner_notified, ^owner, {AdvisoryLocks, :lock_released, ^lock, _reason}}

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      Process.exit(owner, :kill)

      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should unlock a lock reported lost while its session is still alive",
         %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()

      send(@locks, {:connected, lock_connection})
      send(@locks, {:disconnected, lock_connection})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_ended}

      send(@locks, {:connected, lock_connection})

      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should unlock a lock reported lost once a session that could not be verified answers",
         %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()

      send(@locks, {:connected, lock_connection})
      send(@locks, {:disconnected, lock_connection})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_ended}

      # The session still holds the lock but cannot answer the verification query,
      # and it never disconnects, so nothing else would retry the pending unlock.
      Postgrex.query!(@conn, "BEGIN", [])
      assert {:error, _error} = Postgrex.query(@conn, "SELECT 1 / 0", [])

      send(@locks, {:connected, lock_connection})
      :sys.get_state(@locks)

      Postgrex.query!(@conn, "ROLLBACK", [])

      Wait.until(5_000, fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should retry verification for an active lock when a reconnect query fails",
         %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()

      # A reconnect notice makes verification, not the dropped monitor, responsible
      # for the lock. The verification query cannot answer while the session is in a
      # failed transaction, so with only an active lock held it must still schedule a
      # retry rather than leave the owner holding a lock nothing will reconcile.
      Postgrex.query!(@conn, "BEGIN", [])
      assert {:error, _error} = Postgrex.query(@conn, "SELECT 1 / 0", [])

      send(@locks, {:connected, lock_connection})

      assert %{verify_timer: timer} = :sys.get_state(@locks)
      assert is_reference(timer)

      Postgrex.query!(@conn, "ROLLBACK", [])

      # The session was alive all along, so the retry confirms the lock and keeps it.
      refute_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 1_500

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
    end

    test "should send `lock_released` message when a reconnect lands on a new session with the same backend pid" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()

      # A restarted or failed over server can hand the same backend pid to the new
      # session, which then reads a new session id.
      Postgrex.query!(@conn, "SELECT set_config('eventstore.session_id', '', false)", [])

      send(@locks, {:connected, lock_connection})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_ended}, 1_000
    end

    test "should keep retrying verification until a session that cannot answer recovers",
         %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()

      send(@locks, {:connected, lock_connection})
      send(@locks, {:disconnected, lock_connection})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_ended}

      Postgrex.query!(@conn, "BEGIN", [])
      assert {:error, _error} = Postgrex.query(@conn, "SELECT 1 / 0", [])

      send(@locks, {:connected, lock_connection})

      assert %{verify_timer: timer} = :sys.get_state(@locks)
      assert is_reference(timer)

      # The first retry fails too, so only a retry it schedules itself can unlock.
      Wait.until(3_000, fn ->
        assert %{verify_timer: retry} = :sys.get_state(@locks)
        assert is_reference(retry) and retry != timer
      end)

      Postgrex.query!(@conn, "ROLLBACK", [])

      Wait.until(5_000, fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should keep a single verification retry pending while reconnect queries fail" do
      {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()

      Postgrex.query!(@conn, "BEGIN", [])
      assert {:error, _error} = Postgrex.query(@conn, "SELECT 1 / 0", [])

      send(@locks, {:connected, lock_connection})

      assert %{verify_timer: timer} = :sys.get_state(@locks)
      assert is_reference(timer)

      # Each failed verification would otherwise start another polling loop.
      send(@locks, {:connected, lock_connection})

      assert %{verify_timer: ^timer} = :sys.get_state(@locks)

      Postgrex.query!(@conn, "ROLLBACK", [])
    end

    test "should ignore a verification retry that is no longer pending" do
      {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()
      locks = Process.whereis(@locks)

      Postgrex.query!(@conn, "BEGIN", [])
      assert {:error, _error} = Postgrex.query(@conn, "SELECT 1 / 0", [])

      send(@locks, {:connected, lock_connection})

      assert %{verify_timer: timer} = :sys.get_state(@locks)
      assert is_reference(timer)

      # Nothing else talks to the lock server while the retry is pending, so the
      # first message it receives is the retry firing.
      :erlang.trace(locks, true, [:receive])
      assert_receive {:trace, ^locks, :receive, retry}, 3_000
      :erlang.trace(locks, false, [:receive])

      assert %{verify_timer: pending} = :sys.get_state(@locks)
      assert is_reference(pending) and pending != timer

      # A retry that outlived its timer, such as one armed before the lock pool went
      # down, would otherwise start a second polling loop next to the pending one.
      send(@locks, retry)

      assert %{verify_timer: ^pending} = :sys.get_state(@locks)

      Postgrex.query!(@conn, "ROLLBACK", [])
    end

    test "should keep retrying an unlock that fails on a session that answers",
         %{conn: conn, schema: schema} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      # The unlock resolves the subscriptions table by name while the session query
      # does not, so the session is confirmed but its unlock cannot complete.
      Postgrex.query!(conn, "ALTER TABLE subscriptions RENAME TO hidden_subscriptions", [])

      try do
        :ok = AdvisoryLocks.release(@locks, lock)

        Wait.until(fn -> refute Enum.empty?(:sys.get_state(@locks).released) end)
      after
        Postgrex.query!(conn, "ALTER TABLE hidden_subscriptions RENAME TO subscriptions", [])
      end

      Wait.until(5_000, fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    test "should send `lock_released` message when the lock connection crashes" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      without_reconnects(fn lock_connection ->
        Process.exit(lock_connection, :kill)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 500
      end)
    end
  end

  describe "lock pool down" do
    test "should reply to a lock query in flight when the lock pool goes down" do
      reply_to = self()

      holder = hold_only_connection()

      try do
        spawn(fn ->
          send(reply_to, {:lock, AdvisoryLocks.try_advisory_lock(@locks, 1, 15_000)})
        end)

        wait_until_waiting_for_connection()

        Process.exit(lock_pool_pid(), :kill)

        assert_receive {:lock, {:error, _error}}, 1_000
      after
        send(holder, :release)
      end
    end

    test "should not wait for the lock pool to restart" do
      Process.exit(lock_pool_pid(), :kill)

      Wait.until(fn -> assert is_nil(lock_pool_pid()) end)

      assert {:error, _error} = AdvisoryLocks.try_advisory_lock(@locks, 1, 1_000)
    end

    test "should stop retrying verification when the lock pool goes down" do
      {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      lock_connection = lock_connection_pid()

      Postgrex.query!(@conn, "BEGIN", [])
      assert {:error, _error} = Postgrex.query(@conn, "SELECT 1 / 0", [])

      send(@locks, {:connected, lock_connection})

      assert %{verify_timer: timer, ref: ref} = :sys.get_state(@locks)
      assert is_reference(timer)

      send(@locks, {:DOWN, ref, :process, self(), :killed})

      assert %{verify_timer: nil} = :sys.get_state(@locks)
      assert Process.read_timer(timer) == false

      Postgrex.query!(@conn, "ROLLBACK", [])
    end

    test "should keep running when the lock pool monitor does not answer" do
      locks = Process.whereis(@locks)

      :ok = :sys.suspend(@monitor)

      try do
        assert {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1, 10_000)
      after
        :ok = :sys.resume(@monitor)
      end

      assert Process.whereis(@locks) == locks
    end
  end

  describe "graceful shutdown" do
    test "should complete an in-flight lock query before terminating" do
      reply_to = self()

      holder = hold_only_connection()

      log =
        capture_log(fn ->
          spawn(fn ->
            send(reply_to, {:lock, AdvisoryLocks.try_advisory_lock(@locks, 1, 15_000)})
          end)

          wait_until_waiting_for_connection()

          Process.send_after(holder, :release, 200)

          stop_supervised!(TestEventStore)

          assert_receive {:lock, {:ok, lock}}, 15_000
          assert is_reference(lock)
        end)

      refute log =~ "DBConnection.ConnectionError"
    end

    @tag capture_log: true
    test "should stop with the reason reported by a linked process" do
      locks = Process.whereis(@locks)
      ref = Process.monitor(locks)

      spawn(fn ->
        Process.link(locks)
        exit(:boom)
      end)

      assert_receive {:DOWN, ^ref, :process, ^locks, :boom}, 5_000
    end

    test "should keep running when a linked process exits normally" do
      locks = Process.whereis(@locks)
      reply_to = self()

      pid =
        spawn(fn ->
          Process.link(locks)

          send(reply_to, :linked)

          receive do
            :stop -> :ok
          end
        end)

      assert_receive :linked, 5_000

      ref = Process.monitor(locks)

      send(pid, :stop)

      refute_receive {:DOWN, ^ref, :process, ^locks, _reason}, 1_000

      assert {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
    end
  end

  describe "acquire same lock on different schemas" do
    setup do
      postgrex_config = Config.parsed(TestEventStore, :eventstore)

      public_schema =
        postgrex_config
        |> Keyword.put(:schema, "public")
        |> Config.default_postgrex_opts()

      conn1 = start_supervised!({Postgrex, public_schema}, id: :conn1)

      example_schema =
        postgrex_config
        |> Keyword.put(:schema, "example")
        |> Config.default_postgrex_opts()

      conn2 = start_supervised!({Postgrex, example_schema}, id: :conn2)

      [conn1: conn1, conn2: conn2, schema1: "public", schema2: "example"]
    end

    test "should acquire lock", %{conn1: conn1, conn2: conn2, schema1: schema1, schema2: schema2} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn1, 1, schema: schema1)
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn2, 1, schema: schema2)
    end
  end

  describe "lock connection that switches role after connecting" do
    test "should acquire lock and keep running" do
      locks_name = Module.concat([__MODULE__, SwitchedRole, AdvisoryLocks])
      conn_name = Module.concat([locks_name, Postgrex])
      monitor_name = Module.concat([conn_name, MonitoredServer])

      config = Config.parsed(TestEventStore, :eventstore)

      postgrex_config =
        config
        |> Config.advisory_locks_postgrex_opts()
        |> Config.add_connection_listener(locks_name)
        |> Keyword.put(:name, conn_name)
        |> Keyword.put(:after_connect, {Postgrex, :query!, ["SET ROLE pg_signal_backend", []]})

      start_supervised!(
        {MonitoredServer,
         mfa: {Postgrex, :start_link, [postgrex_config]}, name: monitor_name, backoff_min: 30_000},
        id: monitor_name
      )

      locks =
        start_supervised!(
          {AdvisoryLocks,
           conn: conn_name,
           event_store: TestEventStore,
           monitor: monitor_name,
           query_timeout: Keyword.fetch!(config, :timeout),
           schema: "public",
           name: locks_name}
        )

      assert {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks_name, 1)
      assert is_reference(lock)
      assert Process.alive?(locks)
    end
  end

  describe "lock connection that connected before the lock server started" do
    setup do
      locks_name = Module.concat([__MODULE__, ConnectedFirst, AdvisoryLocks])
      conn_name = Module.concat([locks_name, Postgrex])
      monitor_name = Module.concat([conn_name, MonitoredServer])

      config = Config.parsed(TestEventStore, :eventstore)

      postgrex_config =
        config
        |> Config.advisory_locks_postgrex_opts()
        |> Config.add_connection_listener(locks_name)
        |> Keyword.put(:name, conn_name)

      start_supervised!(
        {MonitoredServer,
         mfa: {Postgrex, :start_link, [postgrex_config]}, name: monitor_name, backoff_min: 30_000},
        id: monitor_name
      )

      # The notice for this connection is sent to a lock server that does not exist yet.
      first_session = backend_pid(conn_name)
      first_connection = lock_connection_pid(conn_name)

      [
        locks_name: locks_name,
        conn_name: conn_name,
        monitor_name: monitor_name,
        query_timeout: Keyword.fetch!(config, :timeout),
        first_session: first_session,
        first_connection: first_connection
      ]
    end

    test "should send `lock_released` message when the lock connection crashes", context do
      %{locks_name: locks_name, conn_name: conn_name, first_session: first_session} = context

      start_lock_server(context)

      Wait.until(5_000, fn ->
        refute backend_pid(conn_name) == first_session,
               "the connection made before the lock server started was not replaced"
      end)

      {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks_name, 1)

      without_reconnects(conn_name, fn lock_connection ->
        Process.exit(lock_connection, :kill)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 500
      end)
    end

    test "should send `lock_released` message for a lock it held without waiting for its replacement",
         %{locks_name: locks_name, first_connection: first_connection} = context do
      start_lock_server(context)

      without_restarts(first_connection, fn ->
        # The lock server never heard from this connection, so only its disconnect
        # notice can tell the owner the lock is gone.
        {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks_name, 1)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 500
      end)
    end

    test "should send `lock_released` message for a lock it held when it is killed",
         %{locks_name: locks_name, first_connection: first_connection} = context do
      start_lock_server(context)

      # Suspended, the connection cannot leave before it is killed, so it never sends
      # a disconnect notice and only its replacement can reveal the lock is gone.
      :ok = :sys.suspend(first_connection)

      {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks_name, 1)

      Process.exit(first_connection, :kill)

      assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 5_000
    end
  end

  describe "lock pool down when the lock server starts" do
    @tag capture_log: true
    test "should start even when the lock pool is down" do
      locks_name = Module.concat([__MODULE__, PoolDownAtStart, AdvisoryLocks])
      conn_name = Module.concat([locks_name, Postgrex])
      monitor_name = Module.concat([conn_name, MonitoredServer])

      config = Config.parsed(TestEventStore, :eventstore)

      postgrex_config =
        config
        |> Config.advisory_locks_postgrex_opts()
        |> Config.add_connection_listener(locks_name)
        |> Keyword.put(:name, conn_name)

      start_supervised!(
        {MonitoredServer,
         mfa: {Postgrex, :start_link, [postgrex_config]}, name: monitor_name, backoff_min: 30_000},
        id: monitor_name
      )

      Wait.until(fn -> assert is_pid(Process.whereis(conn_name)) end)

      # The pool is down and its long backoff keeps the name unregistered while the
      # lock server starts, so the startup disconnect has nothing to call.
      Process.exit(Process.whereis(conn_name), :kill)
      Wait.until(fn -> assert is_nil(Process.whereis(conn_name)) end)

      assert {:ok, locks} =
               start_supervised(
                 {AdvisoryLocks,
                  conn: conn_name,
                  event_store: TestEventStore,
                  monitor: monitor_name,
                  query_timeout: Keyword.fetch!(config, :timeout),
                  schema: "public",
                  name: locks_name}
               )

      assert Process.alive?(locks)
    end
  end

  describe "release a lock while the lock connection cannot answer" do
    @tag capture_log: true
    test "should retry the unlock until the lock is released", %{conn: conn, schema: schema} do
      locks_name = start_impatient_lock_server(ReleaseWithoutSession, schema)
      conn_name = Module.concat([locks_name, Postgrex])

      {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks_name, 1)

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      # The only pooled connection is busy, so the release cannot confirm its
      # session and parks the unlock instead of completing it.
      holder = hold_only_connection(conn_name)

      try do
        :ok = AdvisoryLocks.release(locks_name, lock)

        Wait.until(fn -> refute Enum.empty?(:sys.get_state(locks_name).released) end)
      after
        send(holder, :release)
      end

      Wait.until(5_000, fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end

    @tag capture_log: true
    test "should retry the unlock of an owner that terminates until the lock is released",
         %{conn: conn, schema: schema} do
      locks_name = start_impatient_lock_server(OwnerDownWithoutSession, schema)
      conn_name = Module.concat([locks_name, Postgrex])
      reply_to = self()

      owner =
        spawn(fn ->
          send(reply_to, {:lock, AdvisoryLocks.try_advisory_lock(locks_name, 1)})

          receive do
            :shutdown -> :ok
          end
        end)

      assert_receive {:lock, {:ok, _lock}}

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      holder = hold_only_connection(conn_name)

      try do
        send(owner, :shutdown)

        Wait.until(fn -> assert map_size(:sys.get_state(locks_name).locks) == 0 end)
      after
        send(holder, :release)
      end

      Wait.until(5_000, fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end
  end

  describe "lock connection stops answering while a lock query waits on it" do
    @tag capture_log: true
    test "should send `lock_released` message without waiting for the connection to finish closing",
         %{schema: schema} do
      {locks, proxy} =
        start_proxied_lock_store(:stalled_lock_store, schema, idle_interval: 60_000)

      try do
        {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks, 1)

        :ok = EventStore.TcpProxy.freeze(proxy)

        spawn(fn -> AdvisoryLocks.try_advisory_lock(locks, 2, 500) end)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 5_000
      after
        stop_supervised!(EventStore.TcpProxy)
      end
    end

    @tag capture_log: true
    test "should send `lock_released` message before answering the calls queued behind the query",
         %{schema: schema} do
      {locks, proxy} = start_proxied_lock_store(:queued_lock_store, schema, idle_interval: 60_000)
      server = Process.whereis(locks)
      test_process = self()

      try do
        {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks, 1)

        :ok = EventStore.TcpProxy.freeze(proxy)

        spawn(fn -> AdvisoryLocks.try_advisory_lock(locks, 2, 2_000) end)

        Wait.until(fn -> assert {:timeout, _} = catch_exit(:sys.get_state(server, 10)) end)

        spawn(fn ->
          send(test_process, {:queued_reply, AdvisoryLocks.try_advisory_lock(locks, 3, 20_000)})
        end)

        Wait.until(fn ->
          {:messages, messages} = Process.info(server, :messages)

          assert Enum.any?(
                   messages,
                   &match?({:"$gen_call", _from, {:try_advisory_lock, 3, _, _}}, &1)
                 )
        end)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 10_000
        refute_received {:queued_reply, _reply}
      after
        stop_supervised!(EventStore.TcpProxy)
      end
    end
  end

  describe "lock session ends while lock calls are queued and new connections hang" do
    @tag capture_log: true
    test "should send `lock_released` message before the next queued call waits on the pool",
         %{conn: conn, schema: schema} do
      {locks, proxy} =
        start_proxied_lock_store(:failover_lock_store, schema, idle_interval: 60_000)

      lock_pool = Module.concat([locks, Postgrex])

      try do
        {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks, 1)

        lock_connection = lock_connection_pid(lock_pool)
        backend_pid = backend_pid(lock_pool)

        :ok = EventStore.TcpProxy.blackhole(proxy)

        %Postgrex.Result{rows: [[true]]} =
          Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

        # Holds back the connection's notice until the failed call has replied.
        :ok = :sys.suspend(lock_connection)

        for key <- 2..5, do: spawn(fn -> AdvisoryLocks.try_advisory_lock(locks, key) end)

        Wait.until(fn ->
          {:messages, messages} = Process.info(lock_connection, :messages)
          assert messages != []
        end)

        :ok = :sys.resume(lock_connection)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 1_000
      after
        stop_supervised!(EventStore.TcpProxy)
      end
    end
  end

  describe "lock session ends while subscription lock claims are queued and new connections hang" do
    @tag capture_log: true
    test "should send `lock_released` message before a claim that failed to begin retries on the pool",
         %{conn: conn, schema: schema} = context do
      %{subscription_id: subscription_id} = create_subscription(context)

      {locks, proxy} =
        start_proxied_lock_store(:failover_claim_store, schema, idle_interval: 60_000)

      lock_pool = Module.concat([locks, Postgrex])

      try do
        {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks, 1)

        lock_connection = lock_connection_pid(lock_pool)
        backend_pid = backend_pid(lock_pool)

        :ok = EventStore.TcpProxy.blackhole(proxy)

        %Postgrex.Result{rows: [[true]]} =
          Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

        :ok = :sys.suspend(lock_connection)

        for _claim <- 1..4,
            do: spawn(fn -> AdvisoryLocks.try_subscription_lock(locks, subscription_id) end)

        Wait.until(fn ->
          {:messages, messages} = Process.info(lock_connection, :messages)
          assert messages != []
        end)

        :ok = :sys.resume(lock_connection)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 1_000
      after
        stop_supervised!(EventStore.TcpProxy)
      end
    end
  end

  describe "lock session ends while a release is queued and new connections hang" do
    @tag capture_log: true
    test "should send `lock_released` message before a release by reference waits on the pool",
         context do
      lock_released_before_queued_release(context, fn locks, owners ->
        for {_owner, ref} <- owners, do: AdvisoryLocks.release(locks, ref)
      end)
    end

    @tag capture_log: true
    test "should send `lock_released` message before the termination of an owner waits on the pool",
         context do
      lock_released_before_queued_release(context, fn _locks, owners ->
        for {owner, _ref} <- owners, do: Process.exit(owner, :kill)
      end)
    end
  end

  # An event store whose lock connection goes through a proxy that can stop answering.
  defp start_proxied_lock_store(name, schema, session_mode_pool_opts) do
    config = Config.parsed(TestEventStore, :eventstore)

    proxy =
      start_supervised!(
        {EventStore.TcpProxy, {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
      )

    start_supervised!(
      {TestEventStore,
       name: name,
       schema: schema,
       session_mode_pool:
         Keyword.merge(
           [port: EventStore.TcpProxy.port(proxy), timeout: 60_000],
           session_mode_pool_opts
         )}
    )

    {Module.concat([name, AdvisoryLocks]), proxy}
  end

  defp lock_released_before_queued_release(%{schema: schema}, release) do
    {locks, proxy} =
      start_proxied_lock_store(:queued_release_store, schema, idle_interval: 60_000)

    server = Process.whereis(locks)
    test_process = self()

    try do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks, 1)

      owners =
        for key <- 2..5 do
          owner =
            spawn(fn ->
              send(
                test_process,
                {:owner_acquired, self(), AdvisoryLocks.try_advisory_lock(locks, key)}
              )

              Process.sleep(:infinity)
            end)

          assert_receive {:owner_acquired, ^owner, {:ok, ref}}

          {owner, ref}
        end

      :ok = EventStore.TcpProxy.freeze(proxy)

      spawn(fn -> AdvisoryLocks.try_advisory_lock(locks, 6, 500) end)

      Wait.until(fn -> assert {:timeout, _} = catch_exit(:sys.get_state(server, 10)) end)

      release.(locks, owners)

      Wait.until(fn ->
        {:messages, messages} = Process.info(server, :messages)

        queued =
          Enum.count(messages, fn
            {:"$gen_cast", {:release, _ref}} -> true
            {:DOWN, _ref, :process, _owner, _reason} -> true
            _message -> false
          end)

        assert queued == length(owners)
      end)

      assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 1_500
    after
      stop_supervised!(EventStore.TcpProxy)
    end
  end

  # A lock server whose queries give up on a busy connection almost at once.
  defp start_impatient_lock_server(name, schema) do
    alias EventStore.Storage.Session

    locks_name = Module.concat([__MODULE__, name, AdvisoryLocks])
    conn_name = Module.concat([locks_name, Postgrex])
    monitor_name = Module.concat([conn_name, MonitoredServer])

    config = Config.parsed(TestEventStore, :eventstore)

    postgrex_config =
      config
      |> Config.advisory_locks_postgrex_opts()
      |> Config.add_connection_listener(locks_name)
      |> Keyword.put(:name, conn_name)
      |> Keyword.merge(queue_target: 1, queue_interval: 1)

    start_supervised!(
      {MonitoredServer,
       mfa: {Postgrex, :start_link, [postgrex_config]}, name: monitor_name, backoff_min: 30_000},
      id: monitor_name
    )

    start_supervised!(
      {AdvisoryLocks,
       conn: conn_name,
       event_store: TestEventStore,
       monitor: monitor_name,
       query_timeout: 100,
       schema: schema,
       name: locks_name}
    )

    # The startup replacement of the pre-registration connection settles once a
    # session answers, so the lock rides a connection that survives.
    Wait.until(5_000, fn ->
      assert {:ok, _session} = Session.current(conn_name, timeout: 100)
    end)

    locks_name
  end

  defp start_lock_owner(key) do
    test_process = self()

    spawn(fn ->
      send(test_process, {:owner_acquired, self(), AdvisoryLocks.try_advisory_lock(@locks, key)})

      forward_to_test_process(test_process)
    end)
  end

  defp forward_to_test_process(test_process) do
    receive do
      message ->
        send(test_process, {:owner_notified, self(), message})

        forward_to_test_process(test_process)
    end
  end

  defp hold_only_connection(conn \\ lock_pool_pid()) do
    reply_to = self()

    holder =
      spawn(fn ->
        Postgrex.transaction(
          conn,
          fn _ ->
            send(reply_to, :connection_held)

            receive do
              :release -> :ok
            end
          end,
          timeout: 30_000
        )
      end)

    assert_receive :connection_held, 5_000

    holder
  end

  defp postgrex_config do
    Config.parsed(TestEventStore, :eventstore) |> Config.default_postgrex_opts()
  end

  defp lock_subscriptions_table(%{conn: conn, schema: schema}) do
    reply_to = self()

    table_lock =
      Task.async(fn ->
        Postgrex.transaction(conn, fn holder_conn ->
          Postgrex.query!(holder_conn, "LOCK TABLE \"#{schema}\".subscriptions", [])

          send(reply_to, :table_locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :table_locked

    table_lock
  end

  defp release_table_lock(%Task{pid: pid} = table_lock) do
    send(pid, :release)

    {:ok, :ok} = Task.await(table_lock)
  end

  defp wait_until_waiting_for_connection do
    locks = Process.whereis(@locks)

    Wait.until(fn ->
      assert {:timeout, _} = catch_exit(:sys.get_state(locks, 10))
    end)
  end

  defp terminate_lock_session(conn) do
    %Postgrex.Result{rows: [[backend_pid]]} =
      Postgrex.query!(@conn, "SELECT pg_backend_pid()", [])

    %Postgrex.Result{rows: [[true]]} =
      Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

    Wait.until(fn ->
      assert %Postgrex.Result{rows: [[0]]} =
               Postgrex.query!(
                 conn,
                 "SELECT count(*)::int FROM pg_stat_activity WHERE pid = $1",
                 [
                   backend_pid
                 ]
               )
    end)
  end

  defp start_lock_server(context) do
    %{
      locks_name: locks_name,
      conn_name: conn_name,
      monitor_name: monitor_name,
      query_timeout: query_timeout
    } = context

    start_supervised!(
      {AdvisoryLocks,
       conn: conn_name,
       event_store: TestEventStore,
       monitor: monitor_name,
       query_timeout: query_timeout,
       schema: "public",
       name: locks_name}
    )
  end

  defp without_reconnects(conn \\ @conn, fun) do
    lock_connection = lock_connection_pid(conn)

    without_restarts(lock_connection, fn -> fun.(lock_connection) end)
  end

  defp without_restarts(lock_connection, fun) do
    {:dictionary, dictionary} = Process.info(lock_connection, :dictionary)
    [connection_supervisor | _ancestors] = dictionary[:"$ancestors"]

    :ok = :sys.suspend(connection_supervisor)

    try do
      fun.()
    after
      :ok = :sys.resume(connection_supervisor)
    end
  end

  defp lock_connection_pid(conn \\ @conn) do
    backend_pid = backend_pid(conn)

    Enum.find(Process.list(), fn pid ->
      with {:dictionary, dictionary} <- Process.info(pid, :dictionary),
           {DBConnection.Connection, _function, _arity} <- dictionary[:"$initial_call"],
           {_state, %{state: %Postgrex.Protocol{connection_id: ^backend_pid}}} <-
             connection_state(pid) do
        true
      else
        _ -> false
      end
    end)
  end

  defp backend_pid(conn) do
    %Postgrex.Result{rows: [[backend_pid]]} = Postgrex.query!(conn, "SELECT pg_backend_pid()", [])

    backend_pid
  end

  defp connection_state(pid) do
    :sys.get_state(pid, 100)
  catch
    :exit, _reason -> nil
  end

  defp shutdown_database_connection do
    EventStore.ProcessHelper.shutdown(lock_pool_pid())
  end

  defp lock_pool_pid, do: Process.whereis(@conn)

  defp create_subscription(%{conn: conn, schema: schema}, name \\ "test_subscription") do
    {:ok, %Storage.Subscription{} = subscription} =
      Storage.subscribe_to_stream(conn, "$all", name, schema: schema)

    subscription
  end
end
