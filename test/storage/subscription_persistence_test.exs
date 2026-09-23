defmodule EventStore.Storage.SubscriptionPersistenceTest do
  use EventStore.StorageCase

  alias EventStore.Storage

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

    :ok = delete_subscription(context)

    assert {:error, :subscription_not_found} =
             Storage.ack_last_seen_event(conn, subscription_id, 1, schema: schema)
  end

  test "ack last seen event only moves the subscription it names", context do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: acked}} = subscribe_to_stream(context)

    {:ok, %Storage.Subscription{subscription_id: untouched}} =
      Storage.subscribe_to_stream(conn, @all_stream, "another_subscription", schema: schema)

    :ok = Storage.ack_last_seen_event(conn, acked, 1, schema: schema)

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

    :ok = delete_subscription(context)

    {:ok, %Storage.Subscription{subscription_id: recreated}} = subscribe_to_stream(context)

    assert recreated != deleted

    assert {:error, :subscription_not_found} =
             Storage.ack_last_seen_event(conn, deleted, 1, schema: schema)

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
             Storage.ack_last_seen_event(conn, subscription_id, 1, schema: "no_such_schema")
  end

  def ack_last_seen_event(context, last_seen) do
    %{conn: conn, schema: schema} = context

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
      Storage.Subscription.subscription(conn, @all_stream, @subscription_name, schema: schema)

    Storage.ack_last_seen_event(conn, subscription_id, last_seen, schema: schema)
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
