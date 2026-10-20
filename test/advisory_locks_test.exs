defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  import ExUnit.CaptureLog

  alias EventStore.{AdvisoryLocks, Config, MonitoredServer, Wait}
  alias EventStore.Storage

  @locks TestEventStore.EventStore.AdvisoryLocks
  @conn TestEventStore.EventStore.AdvisoryLocks.Postgrex
  @monitor TestEventStore.EventStore.AdvisoryLocks.Postgrex.EventStore.MonitoredServer

  setup do
    postgrex_config = Config.parsed(TestEventStore, :eventstore) |> Config.default_postgrex_opts()

    conn = start_supervised!({Postgrex, postgrex_config})

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
      name = :proxied_lock_store
      locks = Module.concat([name, AdvisoryLocks])
      lock_pool = Module.concat([locks, Postgrex])

      config = Config.parsed(TestEventStore, :eventstore)

      proxy =
        start_supervised!(
          {EventStore.TcpProxy,
           {Keyword.fetch!(config, :hostname), Keyword.get(config, :port, 5432)}}
        )

      start_supervised!(
        {TestEventStore,
         name: name,
         schema: schema,
         session_mode_pool: [
           port: EventStore.TcpProxy.port(proxy),
           timeout: 60_000,
           idle_interval: 100
         ]}
      )

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
        EventStore.TcpProxy.stop(proxy)
      end
    end

    test "should keep a lock acquired while the lost connection is still closing when its disconnect notice arrives" do
      lock_connection = lock_connection_pid()

      send(@locks, {AdvisoryLocks, :session_lost, lock_connection})

      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      send(@locks, {:disconnected, lock_connection})

      refute_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 500
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
    test "should send `lock_released` message when the lock connection crashes" do
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

      start_supervised!(
        {AdvisoryLocks,
         conn: conn_name,
         monitor: monitor_name,
         query_timeout: Keyword.fetch!(config, :timeout),
         schema: "public",
         name: locks_name}
      )

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
      alias EventStore.Storage.Session

      locks_name = Module.concat([__MODULE__, ReleaseWithoutSession, AdvisoryLocks])
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

  defp without_reconnects(conn \\ @conn, fun) do
    lock_connection = lock_connection_pid(conn)

    {:dictionary, dictionary} = Process.info(lock_connection, :dictionary)
    [connection_supervisor | _ancestors] = dictionary[:"$ancestors"]

    :ok = :sys.suspend(connection_supervisor)

    try do
      fun.(lock_connection)
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
