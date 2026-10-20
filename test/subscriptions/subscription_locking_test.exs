defmodule EventStore.Subscriptions.SubscriptionLockingTest do
  use EventStore.StorageCase

  alias EventStore.{Config, EventFactory, Storage, UUID, Wait}
  alias EventStore.Subscriptions.Subscription

  @conn TestEventStore.Postgrex
  @advisory_locks_conn TestEventStore.EventStore.AdvisoryLocks.Postgrex

  setup do
    subscription_name = UUID.uuid4()

    {:ok, %{subscription_name: subscription_name}}
  end

  describe "subscription lock lost" do
    setup [:create_subscription]

    test "should resend in-flight events", %{event_store: event_store, subscription: subscription} do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      :ok = disconnect(subscription)

      # Acknowledgements should be ignored while subscription is disconnected
      :ok = Subscription.ack(subscription, 1)
      :ok = Subscription.ack(subscription, 2)
      :ok = Subscription.ack(subscription, 3)

      :ok = reconnect(subscription)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([1, 2, 3])
    end

    test "should not send ack'd events before disconnect", %{
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      # Acknowledgement sent before disconnect should be persisted
      :ok = Subscription.ack(subscription, 1)

      refute_receive {:events, _received_events}

      :ok = disconnect(subscription)

      # Acknowledgements sent after subscription disconnect should be ignored
      :ok = Subscription.ack(subscription, 2)
      :ok = Subscription.ack(subscription, 3)

      :ok = reconnect(subscription)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([2, 3])
    end

    test "should not send events ack'd by another subscription during disconnect", %{
      event_store: event_store,
      schema: schema,
      subscription: subscription,
      subscription_name: subscription_name
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])
      refute_receive {:events, _received_events}

      :ok = disconnect(subscription)

      {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
        Storage.Subscription.subscription(@conn, "$all", subscription_name, schema: schema)

      :ok = Storage.Subscription.ack_last_seen_event(@conn, subscription_id, 2, schema: schema)

      :ok = reconnect(subscription)

      # Should only receive events not yet ack'd
      assert_receive_events([3])
    end

    test "should subscribe after waiting `retry_interval`", %{
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      :ok = disconnect(subscription)

      # Should receive already sent, but not successfully ack'd events
      assert_receive_events([1, 2, 3])
    end
  end

  describe "subscription lock session ended" do
    setup [:create_subscription]

    test "should resend in-flight events after the lock connection reconnects", %{
      conn: conn,
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      :ok = terminate_lock_session(conn)

      assert_receive_events([1, 2, 3], 5_000)
    end
  end

  describe "duplicate subscriptions" do
    setup [:lock_subscription, :create_subscription]

    test "should not be subscribed", %{subscription: subscription} do
      refute_receive {:subscribed, ^subscription}
    end

    test "should only allow single active subscription", %{
      event_store: event_store,
      subscription: subscription
    } do
      stream1_uuid = append_events_to_stream(event_store, 1)

      # Subscriber should not receive events until subscribed
      refute_receive {:events, _received_events}

      # Release lock, allowing subscriber to subscribe
      stop_supervised!(:subscription_conn)

      # Subscription should now be subscribed
      assert_receive {:subscribed, ^subscription}

      stream2_uuid = append_events_to_stream(event_store, 2)

      # Subscriber should now start receiving events
      received_events = wait_for_events(subscription, 3)

      assert Enum.map(received_events, & &1.stream_uuid) == [
               stream1_uuid,
               stream2_uuid,
               stream2_uuid
             ]

      refute_receive {:events, _received_events}
    end
  end

  describe "late lock notice" do
    setup [:create_subscription]

    test "should keep running when the lock was already released", %{
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      Process.flag(:trap_exit, true)
      monitor_ref = Process.monitor(subscription)

      lock_ref = lock_ref(subscription)

      :ok = release_lock(subscription, lock_ref)
      :sys.get_state(subscription)

      :ok = release_lock(subscription, lock_ref)

      refute_receive {:DOWN, ^monitor_ref, :process, ^subscription, _reason}, 200
      assert Process.alive?(subscription)

      :ok = reconnect(subscription)

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])
    end

    test "should ignore a notice for a lock that was replaced by a new one", %{
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      Process.flag(:trap_exit, true)
      monitor_ref = Process.monitor(subscription)

      old_lock_ref = lock_ref(subscription)

      :ok = release_lock(subscription, old_lock_ref)
      :ok = reconnect(subscription)

      _new_lock_ref = wait_for_new_lock_ref(subscription, old_lock_ref)

      :ok = release_lock(subscription, old_lock_ref)

      refute_receive {:DOWN, ^monitor_ref, :process, ^subscription, _reason}, 200
      assert Process.alive?(subscription)

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])
    end
  end

  defp lock_subscription(context) do
    %{config: config, schema: schema} = context

    config = Config.advisory_locks_postgrex_opts(config)

    conn = start_supervised!({Postgrex, config}, id: :subscription_conn)

    :ok = EventStore.Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

    [conn2: conn]
  end

  defp create_subscription(context) do
    %{
      conn: conn,
      schema: schema,
      event_store: event_store,
      serializer: serializer,
      subscription_name: subscription_name,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = context

    {:ok, subscription} =
      Subscription.start_link(
        event_store: event_store,
        conn: conn,
        schema: schema,
        serializer: serializer,
        correlation_id_type: correlation_id_type,
        causation_id_type: causation_id_type,
        retry_interval: 1_000,
        stream_uuid: "$all",
        subscription_name: subscription_name,
        buffer_size: 3,
        start_from: 0
      )

    {:ok, ^subscription} = Subscription.connect(subscription, self(), buffer_size: 3)

    [subscription: subscription]
  end

  defp terminate_lock_session(conn) do
    %Postgrex.Result{rows: [[backend_pid]]} =
      Postgrex.query!(@advisory_locks_conn, "SELECT pg_backend_pid()", [])

    %Postgrex.Result{rows: [[true]]} =
      Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

    :ok
  end

  defp reconnect(subscription) do
    Process.send(subscription, :subscribe_to_stream, [])
  end

  defp disconnect(subscription) do
    release_lock(subscription, lock_ref(subscription))
  end

  defp lock_ref(subscription) do
    %Subscription{subscription: %{data: %{lock_ref: lock_ref}}} = :sys.get_state(subscription)

    lock_ref
  end

  defp wait_for_new_lock_ref(subscription, old_lock_ref) do
    Wait.until(2_000, fn ->
      lock_ref = lock_ref(subscription)

      assert is_reference(lock_ref) and lock_ref != old_lock_ref

      lock_ref
    end)
  end

  defp release_lock(subscription, lock_ref) do
    Process.send(
      subscription,
      {EventStore.AdvisoryLocks, :lock_released, lock_ref, :shutdown},
      []
    )
  end

  defp append_events_to_stream(event_store, count) do
    stream_uuid = UUID.uuid4()
    stream_events = EventFactory.create_events(count)

    :ok = event_store.append_to_stream(stream_uuid, 0, stream_events)

    stream_uuid
  end

  defp assert_receive_events(expected_event_numbers, timeout \\ 2_000) do
    assert_receive {:events, received_events}, timeout
    assert pluck(received_events, :event_number) == expected_event_numbers
  end

  defp pluck(enumerable, field) do
    Enum.map(enumerable, &Map.get(&1, field))
  end

  defp wait_for_events(subscription, expected_count, events \\ []) do
    assert_receive {:events, received_events}

    :ok = Subscription.ack(subscription, received_events)

    events = events ++ received_events

    if length(events) == expected_count do
      events
    else
      wait_for_events(subscription, expected_count, events)
    end
  end
end
