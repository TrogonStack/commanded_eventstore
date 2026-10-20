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

    :ok = Storage.ack_last_seen_event(conn, subscription_id, 3, schema: schema)

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
end
