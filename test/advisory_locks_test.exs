defmodule EventStore.AdvisoryLocksTest do
  use EventStore.StorageCase

  import ExUnit.CaptureLog

  alias EventStore.{AdvisoryLocks, Config, Wait}
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

  defp shutdown_database_connection do
    %{pid: pid} = Process.whereis(@conn) |> :sys.get_state()

    EventStore.ProcessHelper.shutdown(pid)
  end
end
