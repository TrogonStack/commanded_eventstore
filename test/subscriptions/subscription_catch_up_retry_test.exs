defmodule EventStore.Subscriptions.SubscriptionCatchUpRetryTest do
  use EventStore.StorageCase

  import ExUnit.CaptureLog

  alias EventStore.{EventFactory, RecordedEvent, Storage, UUID}
  alias EventStore.Storage.Lock
  alias EventStore.Subscriptions.{Subscriber, Subscription, SubscriptionFsm, SubscriptionState}

  test "keeps a subscription alive when a catch up read fails and delivers once it recovers" do
    stream_uuid = UUID.uuid4()

    {:ok, subscription} =
      TestEventStore.subscribe_to_stream(stream_uuid, UUID.uuid4(), self(),
        buffer_size: 10,
        retry_interval: 20
      )

    assert_receive {:subscribed, ^subscription}

    schema = put_schema(subscription, "no_such_schema")

    GenServer.cast(subscription, :catch_up)

    refute_receive {:events, _events}, 100
    assert Process.alive?(subscription)

    put_schema(subscription, schema)

    :ok = TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))

    assert_receive {:events, [%RecordedEvent{event_number: 1}, %RecordedEvent{event_number: 2}]},
                   2_000

    assert Process.alive?(subscription)
  end

  test "reads again when the retry timeout arrives" do
    stream_uuid = UUID.uuid4()

    {:ok, subscription} =
      TestEventStore.subscribe_to_stream(stream_uuid, UUID.uuid4(), self(),
        buffer_size: 10,
        retry_interval: 60_000
      )

    assert_receive {:subscribed, ^subscription}

    schema = put_schema(subscription, "no_such_schema")

    GenServer.cast(subscription, :catch_up)

    # The cast queues a second :catch_up for the process itself, so wait for both to run.
    _ = :sys.get_state(subscription)

    %Subscription{subscription: %SubscriptionFsm{state: :catching_up} = fsm} =
      state = :sys.get_state(subscription)

    %SubscriptionFsm{data: %SubscriptionState{retry_timer_ref: ref} = data} = fsm
    assert is_reference(ref)

    :ok = TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))

    recovered = %Subscription{
      state
      | subscription: %SubscriptionFsm{fsm | data: %SubscriptionState{data | schema: schema}}
    }

    assert {:noreply, %Subscription{subscription: %SubscriptionFsm{state: new_state}}} =
             Subscription.handle_info({:timeout, ref, :retry_catch_up}, recovered)

    refute new_state == :catching_up
    assert_receive {:events, [%RecordedEvent{event_number: 1}, %RecordedEvent{event_number: 2}]}
  end

  describe "subscription state machine with a failed catch up read" do
    setup context do
      stream_uuid = UUID.uuid4()
      subscription_name = UUID.uuid4()

      {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
        Storage.subscribe_to_stream(context.conn, stream_uuid, subscription_name,
          schema: context.schema
        )

      {:ok, %Lock.Lease{token: fencing_token}} =
        Lock.claim_subscription_lock(context.conn, subscription_id, schema: context.schema)

      on_exit(fn ->
        :ok = Lock.unlock(context.conn, subscription_id, schema: context.schema)
      end)

      [
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        subscription_id: subscription_id,
        fencing_token: fencing_token
      ]
    end

    test "logs how long it waits before retrying", context do
      fsm = fsm(context, state: :request_catch_up, schema: "no_such_schema", retry_interval: 20)

      log =
        capture_log([level: :warning], fn ->
          assert %SubscriptionFsm{data: %SubscriptionState{retry_timer_ref: ref}} =
                   SubscriptionFsm.catch_up(fsm)

          Process.cancel_timer(ref)
        end)

      assert log =~ ~r/retrying in \d+ms/
    end

    test "clears the retry reference once the retried read succeeds", context do
      failed = failed_read(context, retry_interval: 200)
      %SubscriptionFsm{data: %SubscriptionState{retry_timer_ref: ref}} = failed

      :ok = TestEventStore.append_to_stream(context.stream_uuid, 0, EventFactory.create_events(3))
      assert_receive {:timeout, ^ref, :retry_catch_up}, 2_000

      assert %SubscriptionFsm{data: %SubscriptionState{retry_timer_ref: nil}} =
               failed |> recover_schema(context) |> SubscriptionFsm.retry_catch_up(ref)
    end

    test "an ack during a pending retry does not read again", context do
      {failed, [%RecordedEvent{event_number: first} | _rest]} = delivered_then_failed(context)

      assert %SubscriptionFsm{state: :catching_up, data: %SubscriptionState{retry_timer_ref: ref}} =
               failed |> recover_schema(context) |> SubscriptionFsm.ack(first, self())

      assert is_reference(ref)
      Process.cancel_timer(ref)
    end

    test "acks during a pending retry are recorded", context do
      {failed, events} = delivered_then_failed(context)

      acked =
        Enum.reduce(events, failed, fn %RecordedEvent{event_number: event_number}, fsm ->
          SubscriptionFsm.ack(fsm, event_number, self())
        end)

      assert %SubscriptionFsm{data: %SubscriptionState{in_flight_event_numbers: []} = data} =
               acked

      Process.cancel_timer(data.retry_timer_ref)
    end

    test "a notification without a pending retry does not read", context do
      :ok = TestEventStore.append_to_stream(context.stream_uuid, 0, EventFactory.create_events(3))
      {:ok, events} = TestEventStore.read_stream_forward(context.stream_uuid)

      fsm = in_flight_fsm(context, 1, state: :catching_up)

      assert %SubscriptionFsm{state: :catching_up, data: %SubscriptionState{queue_size: 0}} =
               SubscriptionFsm.notify_events(fsm, events)
    end
  end

  defp delivered_then_failed(context) do
    :ok = TestEventStore.append_to_stream(context.stream_uuid, 0, EventFactory.create_events(10))

    %SubscriptionFsm{data: %SubscriptionState{} = data} =
      context
      |> fsm(state: :request_catch_up, retry_interval: 60_000, checkpoint_threshold: 100)
      |> SubscriptionFsm.connect_subscriber(self(), buffer_size: 10)
      |> SubscriptionFsm.catch_up()

    assert_received {:events, events}
    assert length(events) == 10

    failed =
      SubscriptionFsm.catch_up(%SubscriptionFsm{
        state: :request_catch_up,
        data: %SubscriptionState{data | schema: "no_such_schema"}
      })

    {failed, events}
  end

  defp failed_read(context, overrides) do
    context
    |> fsm(
      [state: :request_catch_up, schema: "no_such_schema", checkpoint_threshold: 100] ++ overrides
    )
    |> SubscriptionFsm.connect_subscriber(self(), buffer_size: 10)
    |> SubscriptionFsm.catch_up()
  end

  defp recover_schema(%SubscriptionFsm{data: %SubscriptionState{} = data} = fsm, context) do
    %SubscriptionFsm{fsm | data: %SubscriptionState{data | schema: context.schema}}
  end

  defp in_flight_fsm(context, count, overrides) do
    %SubscriptionFsm{data: data} = fsm = fsm(context, overrides)

    events = for number <- count..1//-1, do: %RecordedEvent{event_number: number}

    subscriber = %Subscriber{pid: self(), buffer_size: count, in_flight: events, last_sent: count}

    data = %SubscriptionState{
      data
      | subscribers: %{self() => subscriber},
        in_flight_event_numbers: Enum.to_list(1..count),
        last_sent: count,
        last_received: count
    }

    %SubscriptionFsm{fsm | data: data}
  end

  defp fsm(context, overrides) do
    data = %SubscriptionState{
      conn: context.conn,
      schema: Keyword.get(overrides, :schema, context.schema),
      stream_uuid: context.stream_uuid,
      subscription_name: context.subscription_name,
      subscription_id: context.subscription_id,
      fencing_token: context.fencing_token,
      last_ack: 0,
      checkpoints_pending: 0,
      checkpoint_after: 0,
      checkpoint_threshold: Keyword.get(overrides, :checkpoint_threshold, 1),
      retry_interval: Keyword.get(overrides, :retry_interval, 60_000),
      query_timeout: 15_000,
      event_store: TestEventStore,
      serializer: context.serializer,
      correlation_id_type: context.correlation_id_type,
      causation_id_type: context.causation_id_type,
      max_size: 1_000,
      last_received: 0,
      last_sent: 0,
      transient: false
    }

    %SubscriptionFsm{state: Keyword.get(overrides, :state, :subscribed), data: data}
  end

  test "delivers an event appended during a failed read soon after the read recovers" do
    stream_uuid = UUID.uuid4()

    {:ok, subscription} =
      TestEventStore.subscribe_to_stream(stream_uuid, UUID.uuid4(), self(),
        buffer_size: 10,
        checkpoint_threshold: 1_000,
        retry_interval: 60_000
      )

    assert_receive {:subscribed, ^subscription}

    schema = put_schema(subscription, "no_such_schema")

    GenServer.cast(subscription, :catch_up)
    :ok = TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))
    refute_receive {:events, _events}, 50

    put_schema(subscription, schema)

    assert_receive {:events, [%RecordedEvent{event_number: 1}]}, 2_000
  end

  test "resumes an ack-blocked backlog soon after a failed read recovers" do
    stream_uuid = UUID.uuid4()
    :ok = TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(30))

    {:ok, subscription} =
      TestEventStore.subscribe_to_stream(stream_uuid, UUID.uuid4(), self(),
        buffer_size: 10,
        max_size: 5,
        checkpoint_threshold: 1_000,
        retry_interval: 60_000
      )

    assert_receive {:subscribed, ^subscription}
    assert_receive {:events, first}, 2_000

    schema = put_schema(subscription, "no_such_schema")
    :ok = TestEventStore.ack(subscription, first)

    Process.send_after(self(), :recover, 150)
    assert_receive :recover, 1_000
    put_schema(subscription, schema)

    last = receive_and_ack(subscription, List.last(first).event_number)
    assert last == 30
  end

  defp receive_and_ack(_subscription, 30), do: 30

  defp receive_and_ack(subscription, _last) do
    assert_receive {:events, events}, 2_000
    :ok = TestEventStore.ack(subscription, events)

    receive_and_ack(subscription, List.last(events).event_number)
  end

  defp put_schema(subscription, schema) do
    me = self()

    :sys.replace_state(subscription, fn %Subscription{subscription: fsm} = state ->
      %{data: %SubscriptionState{schema: previous} = data} = fsm
      send(me, {:previous_schema, previous})

      %Subscription{
        state
        | subscription: %{fsm | data: %SubscriptionState{data | schema: schema}}
      }
    end)

    assert_receive {:previous_schema, previous}

    previous
  end
end
