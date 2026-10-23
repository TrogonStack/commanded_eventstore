defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  import ExUnit.CaptureLog

  alias EventStore.{AdvisoryLocks, Config, MonitoredServer, Wait}
  alias EventStore.Storage

  @locks TestEventStore.EventStore.AdvisoryLocks
  @conn TestEventStore.EventStore.AdvisoryLocks.Postgrex

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

    test "should acquire lock when same process already has lock" do
      assert {:ok, lock1} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock2} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock3} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert is_reference(lock1)
      assert is_reference(lock2)
      assert is_reference(lock3)
    end

    test "should fail to acquire lock when already taken", %{conn: conn, schema: schema} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      assert {:error, :lock_already_taken} = AdvisoryLocks.try_advisory_lock(@locks, 1)
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

      assert_receive({AdvisoryLocks, :lock_released, ^lock, :shutdown})
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

    test "should send `lock_released` message when the lock connection crashes" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      without_reconnects(fn lock_connection ->
        Process.exit(lock_connection, :kill)

        assert_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 500
      end)
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

      config = Config.parsed(TestEventStore, :eventstore)

      postgrex_config =
        config
        |> Config.advisory_locks_postgrex_opts()
        |> Config.add_connection_listener(locks_name)
        |> Keyword.put(:after_connect, {Postgrex, :query!, ["SET ROLE pg_signal_backend", []]})

      start_supervised!(
        {MonitoredServer,
         mfa: {Postgrex, :start_link, [postgrex_config]}, name: conn_name, backoff_min: 30_000},
        id: conn_name
      )

      locks =
        start_supervised!(
          {AdvisoryLocks,
           conn: conn_name,
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

      config = Config.parsed(TestEventStore, :eventstore)

      postgrex_config =
        config
        |> Config.advisory_locks_postgrex_opts()
        |> Config.add_connection_listener(locks_name)

      start_supervised!(
        {MonitoredServer,
         mfa: {Postgrex, :start_link, [postgrex_config]}, name: conn_name, backoff_min: 30_000},
        id: conn_name
      )

      # The notice for this connection is sent to a lock server that does not exist yet.
      first_session = backend_pid(conn_name)

      start_supervised!(
        {AdvisoryLocks,
         conn: conn_name,
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

  defp hold_only_connection do
    %{pid: conn} = Process.whereis(@conn) |> :sys.get_state()

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
    %{pid: pid} = Process.whereis(@conn) |> :sys.get_state()

    EventStore.ProcessHelper.shutdown(pid)
  end
end
