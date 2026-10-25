defmodule EventStore.Storage.SubscriptionPersistenceTest do
  use EventStore.StorageCase

  alias EventStore.Storage
  alias EventStore.Storage.Lock
  alias EventStore.Subscriptions.FencingToken
  alias EventStore.Wait

  @all_stream "$all"
  @subscription_name "test_subscription"

  test "create subscription", context do
    {:ok, subscription} = subscribe_to_stream(context)

    verify_subscription(subscription)
  end

  test "create subscription when already exists", context do
    {:ok, subscription1} = subscribe_to_stream(context)
    {:ok, subscription2} = subscribe_to_stream(context)

    verify_subscription(subscription1)
    verify_subscription(subscription2)

    assert subscription1.subscription_id == subscription2.subscription_id
  end

  test "list subscriptions", context do
    {:ok, subscription} = subscribe_to_stream(context)
    {:ok, subscriptions} = list_subscriptions(context)

    assert length(subscriptions) > 0
    assert Enum.member?(subscriptions, subscription)
  end

  test "remove subscription when exists", context do
    {:ok, subscriptions} = list_subscriptions(context)
    initial_length = length(subscriptions)

    {:ok, _subscription} = subscribe_to_stream(context)
    :ok = delete_subscription(context)

    {:ok, subscriptions} = list_subscriptions(context)
    assert length(subscriptions) == initial_length
  end

  test "remove subscription when not found should succeed", context do
    :ok = delete_subscription(context)
  end

  test "ack last seen event by id", context do
    {:ok, _subscription} = subscribe_to_stream(context)

    :ok = ack_last_seen_event(context, 1)

    {:ok, subscriptions} = list_subscriptions(context)

    subscription = subscriptions |> Enum.reverse() |> hd

    verify_subscription(subscription, 1)
  end

  test "ack last seen event by stream version", context do
    {:ok, _subscription} = subscribe_to_stream(context)

    :ok = ack_last_seen_event(context, 1)

    {:ok, subscriptions} = list_subscriptions(context)

    subscription = subscriptions |> Enum.reverse() |> hd

    verify_subscription(subscription, 1)
  end

  test "ack last seen event when the subscription no longer exists", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} = subscribe_to_stream(context)

    token = claim_token(context, subscription_id)

    :ok = delete_subscription(context)

    assert {:error, :subscription_not_found} =
             Storage.ack_last_seen_event(conn, token, 1, schema: schema)
  end

  test "ack last seen event only moves the subscription it names", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: acked}} = subscribe_to_stream(context)

    {:ok, %Storage.Subscription{subscription_id: untouched}} =
      Storage.subscribe_to_stream(conn, @all_stream, "another_subscription", schema: schema)

    :ok = Storage.ack_last_seen_event(conn, claim_token(context, acked), 1, schema: schema)

    assert {:ok, %Storage.Subscription{subscription_id: ^acked, last_seen: 1}} =
             Storage.Subscription.subscription(conn, @all_stream, @subscription_name,
               schema: schema
             )

    assert {:ok, %Storage.Subscription{subscription_id: ^untouched, last_seen: nil}} =
             Storage.Subscription.subscription(conn, @all_stream, "another_subscription",
               schema: schema
             )
  end

  test "ack last seen event when the name now belongs to a different subscription", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: deleted}} = subscribe_to_stream(context)

    deleted_token = claim_token(context, deleted)

    :ok = delete_subscription(context)

    {:ok, %Storage.Subscription{subscription_id: recreated}} = subscribe_to_stream(context)

    assert recreated != deleted

    assert {:error, :subscription_not_found} =
             Storage.ack_last_seen_event(conn, deleted_token, 1, schema: schema)

    # The stream and name are the same as the deleted subscription's, so anything keyed by those
    # would have credited this subscription with a position it never acknowledged.
    assert {:ok, %Storage.Subscription{subscription_id: ^recreated, last_seen: nil}} =
             Storage.Subscription.subscription(conn, @all_stream, @subscription_name,
               schema: schema
             )
  end

  test "ack last seen event reports a storage failure as itself, not as a missing subscription",
       context do
    %{conn: conn} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} = subscribe_to_stream(context)

    assert {:error, %Postgrex.Error{}} =
             Storage.ack_last_seen_event(conn, claim_token(context, subscription_id), 1,
               schema: "no_such_schema"
             )
  end

  test "ack last seen event for a subscription that never existed", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} = subscribe_to_stream(context)

    assert {:error, :subscription_not_found} =
             Storage.ack_last_seen_event(conn, FencingToken.new(subscription_id + 1_000, 1), 1,
               schema: schema
             )

    assert {:ok, %Storage.Subscription{last_seen: nil}} = read_subscription(context)
  end

  test "ack last seen event twice at the same position", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} = subscribe_to_stream(context)

    token = claim_token(context, subscription_id)

    assert :ok = Storage.ack_last_seen_event(conn, token, 3, schema: schema)

    # Postgres counts the rows it matched rather than the rows it changed, so an acknowledgement
    # that moves nothing is still an acknowledgement its row accepted.
    assert :ok = Storage.ack_last_seen_event(conn, token, 3, schema: schema)

    assert {:ok, %Storage.Subscription{last_seen: 3}} = read_subscription(context)
  end

  test "ack last seen event at the position a subscription starts from", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} = subscribe_to_stream(context)

    token = claim_token(context, subscription_id)

    assert :ok = Storage.ack_last_seen_event(conn, token, 0, schema: schema)

    # Zero is a position that was acknowledged, where null is a subscription that never has.
    assert {:ok, %Storage.Subscription{last_seen: 0}} = read_subscription(context)
  end

  test "ack last seen event leaves everything but the position alone", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{} = before} = subscribe_to_stream(context)

    token = claim_token(context, before.subscription_id)

    :ok = Storage.ack_last_seen_event(conn, token, 3, schema: schema)

    assert {:ok, %Storage.Subscription{} = after_ack} = read_subscription(context)

    assert after_ack == %Storage.Subscription{before | last_seen: 3}
  end

  test "ack last seen event late does not roll the position back", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} = subscribe_to_stream(context)

    token = claim_token(context, subscription_id)

    :ok = Storage.ack_last_seen_event(conn, token, 10, schema: schema)
    :ok = Storage.ack_last_seen_event(conn, token, 5, schema: schema)

    assert {:ok, %Storage.Subscription{last_seen: 10}} = read_subscription(context)
  end

  test "ack last seen event racing a delete reports the subscription as gone", context do
    %{conn: conn, schema: schema, postgrex_config: postgrex_config} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} = subscribe_to_stream(context)

    token = claim_token(context, subscription_id)

    deleter = start_supervised!({Postgrex, postgrex_config}, id: :deleter)
    observer = start_supervised!({Postgrex, postgrex_config}, id: :observer)

    test = self()

    delete =
      Task.async(fn ->
        Postgrex.transaction(deleter, fn deleter ->
          :ok =
            Storage.delete_subscription(deleter, @all_stream, @subscription_name, schema: schema)

          send(test, :deleted)

          receive do
            :commit -> :ok
          end
        end)
      end)

    assert_receive :deleted

    ack = Task.async(fn -> Storage.ack_last_seen_event(conn, token, 1, schema: schema) end)

    Wait.until(5_000, fn ->
      assert %Postgrex.Result{rows: [[1]]} =
               Postgrex.query!(
                 observer,
                 "SELECT count(*)::int FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'",
                 []
               )
    end)

    send(delete.pid, :commit)

    assert {:ok, :ok} = Task.await(delete)
    assert {:error, :subscription_not_found} = Task.await(ack)
    assert {:error, :subscription_not_found} = read_subscription(context)
  end

  defp read_subscription(context) do
    %{conn: conn, schema: schema} = context

    Storage.Subscription.subscription(conn, @all_stream, @subscription_name, schema: schema)
  end

  def ack_last_seen_event(context, last_seen) do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
      Storage.Subscription.subscription(conn, @all_stream, @subscription_name, schema: schema)

    token = claim_token(context, subscription_id)

    Storage.ack_last_seen_event(conn, token, last_seen, schema: schema)
  end

  defp claim_token(%{conn: conn, schema: schema}, subscription_id) do
    {:ok, %Lock.Lease{token: token}} =
      Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    token
  end

  defp subscribe_to_stream(context) do
    %{conn: conn, schema: schema} = context

    Storage.subscribe_to_stream(conn, @all_stream, @subscription_name, schema: schema)
  end

  defp delete_subscription(context) do
    %{conn: conn, schema: schema} = context

    Storage.delete_subscription(conn, @all_stream, @subscription_name, schema: schema)
  end

  defp list_subscriptions(context) do
    %{conn: conn, schema: schema} = context

    Storage.subscriptions(conn, schema: schema)
  end

  defp verify_subscription(subscription, last_seen \\ nil)

  defp verify_subscription(subscription, last_seen) do
    assert subscription.subscription_id > 0
    assert subscription.stream_uuid == @all_stream
    assert subscription.subscription_name == @subscription_name
    assert subscription.last_seen == last_seen
    assert subscription.created_at != nil
  end
end
