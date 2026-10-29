defmodule EventStore.Subscriptions.SubscriptionCatchUpRetryTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, RecordedEvent, UUID}
  alias EventStore.Subscriptions.{Subscription, SubscriptionState}

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
