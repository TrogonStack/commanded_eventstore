defmodule EventStore.Storage.SubscriptionLockTest do
  use EventStore.StorageCase

  alias EventStore.ProcessHelper
  alias EventStore.Storage
  alias EventStore.Storage.Lock
  alias EventStore.Storage.Session
  alias EventStore.Subscriptions.FencingToken

  @all_stream "$all"
  @subscription_name "test_subscription"

  setup %{postgrex_config: postgrex_config} do
    [conn: start_supervised!({Postgrex, postgrex_config})]
  end

  test "a new subscription has never had its lock claimed", context do
    %{subscription_id: subscription_id} = create_subscription(context)

    assert 0 == lock_generation(context, subscription_id)
  end

  test "claim the lock of a new subscription", context do
    %{conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    assert {:ok, lease} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    assert is_struct(lease, Lock.Lease)
    assert lease.token == FencingToken.new(subscription_id, 1)
    assert lease.last_seen == nil
    assert %Session{} = lease.session
    assert 1 == lock_generation(context, subscription_id)
  end

  test "claim the lock resumes from the committed position", context do
    %{conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    {:ok, %{token: token}} = Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    :ok = Storage.ack_last_seen_event(conn, token, 3, schema: schema)

    assert {:ok, %{last_seen: 3}} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)
  end

  test "claim the lock again from the same connection", context do
    %{conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    assert {:ok, %{token: %{generation: 1}}} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    assert {:ok, %{token: %{generation: 2}}} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)
  end

  test "claim the lock while another connection holds it", context do
    %{postgrex_config: postgrex_config, conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    {:ok, owner} = Postgrex.start_link(postgrex_config)

    assert {:ok, _lease} =
             Lock.claim_subscription_lock(owner, subscription_id, schema: schema)

    assert {:error, :lock_already_taken} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    assert 1 == lock_generation(context, subscription_id)

    ProcessHelper.shutdown(owner)
  end

  test "claim the lock of a subscription that no longer exists", context do
    %{postgrex_config: postgrex_config, conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    :ok = Storage.delete_subscription(conn, @all_stream, @subscription_name, schema: schema)

    assert {:error, :subscription_not_found} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    {:ok, other} = Postgrex.start_link(postgrex_config)

    assert :ok = Lock.try_acquire_exclusive_lock(other, subscription_id, schema: schema)

    ProcessHelper.shutdown(other)
  end

  test "a claim that cannot lock the subscription row leaves the subscription claimable",
       context do
    %{postgrex_config: postgrex_config, conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    claiming_conn =
      start_supervised!(
        {Postgrex,
         Keyword.merge(postgrex_config, parameters: [lock_timeout: "200ms"], pool_size: 1)},
        id: :claiming_conn
      )

    test_pid = self()

    holder =
      Task.async(fn ->
        Postgrex.transaction(conn, fn holder_conn ->
          Postgrex.query!(
            holder_conn,
            "SELECT subscription_id FROM \"#{schema}\".subscriptions WHERE subscription_id = $1 FOR UPDATE",
            [subscription_id]
          )

          send(test_pid, :row_locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :row_locked

    assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
             Lock.claim_subscription_lock(claiming_conn, subscription_id, schema: schema)

    send(holder.pid, :release)
    assert {:ok, :ok} = Task.await(holder)

    assert {:ok, %Lock.Lease{}} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    assert :ok = Lock.unlock(conn, subscription_id, schema: schema)
  end

  test "a claim gives up on a locked subscription row at the stricter session lock timeout",
       context do
    %{postgrex_config: postgrex_config, conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    claiming_conn =
      start_supervised!(
        {Postgrex,
         Keyword.merge(postgrex_config, parameters: [lock_timeout: "100ms"], pool_size: 1)},
        id: :claiming_conn
      )

    test_pid = self()

    holder =
      Task.async(fn ->
        Postgrex.transaction(conn, fn holder_conn ->
          Postgrex.query!(
            holder_conn,
            "SELECT subscription_id FROM \"#{schema}\".subscriptions WHERE subscription_id = $1 FOR UPDATE",
            [subscription_id]
          )

          send(test_pid, :row_locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :row_locked

    started_at = System.monotonic_time(:millisecond)

    assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
             Lock.claim_subscription_lock(claiming_conn, subscription_id,
               schema: schema,
               timeout: 6_000
             )

    assert System.monotonic_time(:millisecond) - started_at < 2_000

    send(holder.pid, :release)
    assert {:ok, :ok} = Task.await(holder)
  end

  test "a claim that loses does not wait on the row of the subscription", context do
    %{postgrex_config: postgrex_config, conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    {:ok, owner} = Postgrex.start_link(postgrex_config)

    assert {:ok, _lease} = Lock.claim_subscription_lock(owner, subscription_id, schema: schema)

    claiming_conn =
      start_supervised!(
        {Postgrex,
         Keyword.merge(postgrex_config, parameters: [lock_timeout: "200ms"], pool_size: 1)},
        id: :claiming_conn
      )

    test_pid = self()

    holder =
      Task.async(fn ->
        Postgrex.transaction(conn, fn holder_conn ->
          Postgrex.query!(
            holder_conn,
            "SELECT subscription_id FROM \"#{schema}\".subscriptions WHERE subscription_id = $1 FOR UPDATE",
            [subscription_id]
          )

          send(test_pid, :row_locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :row_locked

    assert {:error, :lock_already_taken} =
             Lock.claim_subscription_lock(claiming_conn, subscription_id, schema: schema)

    send(holder.pid, :release)
    assert {:ok, :ok} = Task.await(holder)

    ProcessHelper.shutdown(owner)
  end

  test "a claim that loses does not spend a transaction id", context do
    %{postgrex_config: postgrex_config, conn: conn, schema: schema} = context
    %{subscription_id: subscription_id} = create_subscription(context)

    {:ok, owner} = Postgrex.start_link(postgrex_config)
    {:ok, claimer} = Postgrex.start_link(Keyword.put(postgrex_config, :pool_size, 1))

    assert {:ok, _lease} = Lock.claim_subscription_lock(owner, subscription_id, schema: schema)

    %Postgrex.Result{rows: [[claimer_pid]]} =
      Postgrex.query!(claimer, "SELECT pg_backend_pid()", [])

    test_pid = self()

    observe_open_transaction = fn _entry ->
      %Postgrex.Result{rows: [[xid]]} =
        Postgrex.query!(
          conn,
          "SELECT backend_xid::text FROM pg_stat_activity WHERE pid = $1",
          [claimer_pid]
        )

      send(test_pid, {:backend_xid, xid})
    end

    for _ <- 1..5 do
      assert {:error, :lock_already_taken} =
               Lock.claim_subscription_lock(claimer, subscription_id,
                 schema: schema,
                 log: observe_open_transaction
               )
    end

    observed = drain_backend_xids()

    assert observed != []
    assert Enum.all?(observed, &is_nil/1)

    ProcessHelper.shutdown(owner)
  end

  describe "ack with a fencing token" do
    test "a stale generation is fenced and leaves the position alone", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, %{token: stale}} =
        Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

      {:ok, %{token: current}} =
        Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

      assert :ok = Storage.ack_last_seen_event(conn, current, 3, schema: schema)

      assert {:error, :fenced} = Storage.ack_last_seen_event(conn, stale, 7, schema: schema)

      assert 3 == last_seen(context, subscription_id)
    end

    test "the current generation is accepted", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, %{token: token}} =
        Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

      assert :ok = Storage.ack_last_seen_event(conn, token, 4, schema: schema)

      assert 4 == last_seen(context, subscription_id)
    end

    test "a subscription that no longer exists is not found rather than fenced", context do
      %{conn: conn, schema: schema} = context
      %{subscription_id: subscription_id} = create_subscription(context)

      {:ok, %{token: token}} =
        Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

      :ok = Storage.delete_subscription(conn, @all_stream, @subscription_name, schema: schema)

      assert {:error, :subscription_not_found} =
               Storage.ack_last_seen_event(conn, token, 1, schema: schema)
    end
  end

  defp create_subscription(%{conn: conn, schema: schema}) do
    {:ok, %Storage.Subscription{} = subscription} =
      Storage.subscribe_to_stream(conn, @all_stream, @subscription_name, schema: schema)

    subscription
  end

  defp lock_generation(%{conn: conn, schema: schema}, subscription_id) do
    %Postgrex.Result{rows: [[generation]]} =
      Postgrex.query!(
        conn,
        "SELECT lock_generation FROM \"#{schema}\".subscriptions WHERE subscription_id = $1",
        [subscription_id]
      )

    generation
  end

  defp last_seen(%{conn: conn, schema: schema}, subscription_id) do
    %Postgrex.Result{rows: [[last_seen]]} =
      Postgrex.query!(
        conn,
        "SELECT last_seen FROM \"#{schema}\".subscriptions WHERE subscription_id = $1",
        [subscription_id]
      )

    last_seen
  end

  defp drain_backend_xids(acc \\ []) do
    receive do
      {:backend_xid, xid} -> drain_backend_xids([xid | acc])
    after
      0 -> acc
    end
  end
end
