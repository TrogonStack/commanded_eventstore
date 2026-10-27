defmodule EventStore.Subscriptions.SubscriptionLockingTest do
  use EventStore.StorageCase

  import ExUnit.CaptureLog

  alias EventStore.{Config, EventFactory, RecordedEvent, Storage, UUID, Wait}
  alias EventStore.Subscriptions.{FencingToken, Subscription}

  @conn TestEventStore.Postgrex
  @advisory_locks TestEventStore.EventStore.AdvisoryLocks
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

      token = FencingToken.new(subscription_id, lock_generation(subscription_id, schema))

      :ok = Storage.Subscription.ack_last_seen_event(@conn, token, 2, schema: schema)

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

      new_lock_ref = wait_for_new_lock_ref(subscription, old_lock_ref)

      Wait.until(fn -> assert :subscribed == subscription_state(subscription) end)

      :ok = release_lock(subscription, old_lock_ref)

      refute_receive {:DOWN, ^monitor_ref, :process, ^subscription, _reason}, 200
      assert Process.alive?(subscription)

      assert :subscribed == subscription_state(subscription)
      assert new_lock_ref == lock_ref(subscription)

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])
    end
  end

  describe "late fenced notice" do
    setup [:create_subscription]

    test "should keep running when the lock was already released", %{
      event_store: event_store,
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      monitor_ref = Process.monitor(subscription)

      lock_ref = lock_ref(subscription)

      :ok = disconnect(subscription)
      :sys.get_state(subscription)

      send(subscription, {:fenced, lock_ref})

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

      monitor_ref = Process.monitor(subscription)

      old_lock_ref = lock_ref(subscription)

      :ok = disconnect(subscription)
      :ok = reconnect(subscription)

      new_lock_ref = wait_for_new_lock_ref(subscription, old_lock_ref)

      Wait.until(fn -> assert :subscribed == subscription_state(subscription) end)

      send(subscription, {:fenced, old_lock_ref})

      refute_receive {:DOWN, ^monitor_ref, :process, ^subscription, _reason}, 200
      assert Process.alive?(subscription)

      assert :subscribed == subscription_state(subscription)
      assert new_lock_ref == lock_ref(subscription)

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])
    end
  end

  describe "subscription retrying while disconnected" do
    @describetag retry_interval: 60_000

    setup [:create_subscription]

    test "keeps a single retry pending however many events it hears about", %{
      subscription: subscription
    } do
      assert_receive {:subscribed, ^subscription}

      trace_retries(subscription)

      :ok = disconnect(subscription)

      for event_number <- 1..10 do
        send(subscription, {:events, [%RecordedEvent{event_number: event_number}]})
      end

      assert retries_scheduled(subscription) == 1
    end
  end

  describe "acknowledgement sent before a disconnect" do
    @describetag retry_interval: 60_000

    setup context do
      {:ok, subscription} = start_subscription(context)

      [subscription: subscription]
    end

    test "does not cover events redelivered after the reconnect", %{
      event_store: event_store,
      subscription: subscription
    } do
      {departed, survivor} =
        connect_departing_and_surviving_subscribers(subscription, event_store)

      :ok = disconnect(subscription)

      depart(subscription, departed)

      :ok = reconnect(subscription)

      Wait.until(5_000, fn -> assert in_flight(subscription, survivor) == [2, 1] end)

      # The survivor acknowledges the delivery of event 2 it received before the disconnect, which
      # says nothing about event 1 it has only just been sent.
      :ok = Subscription.ack(subscription, 2, survivor)

      assert Subscription.last_seen(subscription) == 0
      assert in_flight(subscription, survivor) == [2, 1]

      :ok = Subscription.ack(subscription, 2, survivor)

      assert Subscription.last_seen(subscription) == 2
    end

    test "does not cover events redelivered after the reconnect when it arrives while disconnected",
         %{event_store: event_store, subscription: subscription} do
      {departed, survivor} =
        connect_departing_and_surviving_subscribers(subscription, event_store)

      :ok = disconnect(subscription)

      depart(subscription, departed)

      :ok = Subscription.ack(subscription, 2, survivor)

      :ok = reconnect(subscription)

      Wait.until(5_000, fn -> assert in_flight(subscription, survivor) == [2, 1] end)

      :ok = Subscription.ack(subscription, 2, survivor)

      assert Subscription.last_seen(subscription) == 2
    end
  end

  describe "subscription stopped after another owner took over" do
    test "should leave the progress of the new owner untouched", %{
      conn: conn,
      event_store: event_store,
      schema: schema,
      subscription_name: subscription_name
    } do
      {:ok, subscription} =
        event_store.subscribe_to_all_streams(subscription_name, self(),
          buffer_size: 3,
          checkpoint_threshold: 100
        )

      assert_receive {:subscribed, ^subscription}

      append_events_to_stream(event_store, 3)

      assert_receive_events([1, 2, 3])

      Postgrex.query!(
        conn,
        "UPDATE \"#{schema}\".subscriptions SET lock_generation = lock_generation + 1, last_seen = 2 WHERE subscription_name = $1",
        [subscription_name]
      )

      :ok = Subscription.ack(subscription, 3)

      monitor_ref = Process.monitor(subscription)

      log =
        capture_log(fn ->
          assert :ok = GenServer.stop(subscription)
        end)

      assert_receive {:DOWN, ^monitor_ref, :process, ^subscription, :normal}
      refute log =~ "terminating"

      assert 2 == last_seen(conn, schema, subscription_name)
    end
  end

  defp last_seen(conn, schema, subscription_name) do
    %Postgrex.Result{rows: [[last_seen]]} =
      Postgrex.query!(
        conn,
        "SELECT last_seen FROM \"#{schema}\".subscriptions WHERE subscription_name = $1",
        [subscription_name]
      )

    last_seen
  end

  defp lock_subscription(context) do
    %{config: config, schema: schema} = context

    config = Config.advisory_locks_postgrex_opts(config)

    conn = start_supervised!({Postgrex, config}, id: :subscription_conn)

    :ok = EventStore.Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

    [conn2: conn]
  end

  defp create_subscription(context) do
    {:ok, subscription} = start_subscription(context)

    {:ok, ^subscription} = Subscription.connect(subscription, self(), buffer_size: 3)

    [subscription: subscription]
  end

  defp start_subscription(context) do
    %{
      conn: conn,
      schema: schema,
      event_store: event_store,
      serializer: serializer,
      subscription_name: subscription_name,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = context

    Subscription.start_link(
      event_store: event_store,
      conn: conn,
      schema: schema,
      serializer: serializer,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type,
      retry_interval: Map.get(context, :retry_interval, 1_000),
      stream_uuid: "$all",
      subscription_name: subscription_name,
      buffer_size: 3,
      start_from: 0
    )
  end

  # Leaves the departing subscriber holding event 1 and the surviving one holding event 2, so the
  # survivor is handed both once the departed subscriber is gone.
  defp connect_departing_and_surviving_subscribers(subscription, event_store) do
    departed = idle_subscriber()
    survivor = idle_subscriber()

    {:ok, ^subscription} =
      Subscription.connect(subscription, departed, buffer_size: 1, concurrency_limit: 2)

    Wait.until(5_000, fn -> assert subscription_state(subscription) == :subscribed end)

    append_events_to_stream(event_store, 1)

    Wait.until(5_000, fn -> assert in_flight(subscription, departed) == [1] end)

    {:ok, ^subscription} =
      Subscription.connect(subscription, survivor, buffer_size: 2, concurrency_limit: 2)

    append_events_to_stream(event_store, 1)

    Wait.until(5_000, fn -> assert in_flight(subscription, survivor) == [2] end)

    {departed, survivor}
  end

  defp idle_subscriber do
    subscriber = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn -> Process.exit(subscriber, :kill) end)

    subscriber
  end

  defp depart(subscription, subscriber) do
    Process.exit(subscriber, :kill)

    Wait.until(5_000, fn -> refute Map.has_key?(subscribers(subscription), subscriber) end)
  end

  defp in_flight(subscription, subscriber) do
    %{^subscriber => %{in_flight: in_flight}} = subscribers(subscription)

    pluck(in_flight, :event_number)
  end

  defp subscribers(subscription) do
    %Subscription{subscription: %{data: %{subscribers: subscribers}}} =
      :sys.get_state(subscription)

    subscribers
  end

  defp terminate_lock_session(conn) do
    %Postgrex.Result{rows: [[backend_pid]]} =
      Postgrex.query!(@advisory_locks_conn, "SELECT pg_backend_pid()", [])

    %Postgrex.Result{rows: [[true]]} =
      Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

    :ok
  end

  defp lock_generation(subscription_id, schema) do
    %Postgrex.Result{rows: [[generation]]} =
      Postgrex.query!(
        @conn,
        "SELECT lock_generation FROM \"#{schema}\".subscriptions WHERE subscription_id = $1",
        [subscription_id]
      )

    generation
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

  defp subscription_state(subscription) do
    %Subscription{subscription: %{state: state}} = :sys.get_state(subscription)

    state
  end

  defp trace_retries(subscription) do
    :erlang.trace_pattern({:erlang, :send_after, :_}, true, [:global])
    :erlang.trace(subscription, true, [:call])

    on_exit(fn -> :erlang.trace_pattern({:erlang, :send_after, :_}, false, [:global]) end)
  end

  defp retries_scheduled(subscription) do
    _state = :sys.get_state(subscription)

    ref = :erlang.trace_delivered(subscription)
    assert_receive {:trace_delivered, ^subscription, ^ref}

    count_retries(subscription, 0)
  end

  defp count_retries(subscription, count) do
    receive do
      {:trace, ^subscription, :call,
       {:erlang, :send_after, [_time, _dest, :subscribe_to_stream | _opts]}} ->
        count_retries(subscription, count + 1)
    after
      0 -> count
    end
  end

  defp wait_for_new_lock_ref(subscription, old_lock_ref) do
    Wait.until(2_000, fn ->
      lock_ref = lock_ref(subscription)

      assert is_reference(lock_ref) and lock_ref != old_lock_ref

      lock_ref
    end)
  end

  defp release_lock(subscription, lock_ref) do
    :ok = EventStore.AdvisoryLocks.release(@advisory_locks, lock_ref)
    _state = :sys.get_state(@advisory_locks)

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
