defmodule EventStore.Subscriptions.SubscriberTest do
  use ExUnit.Case, async: true

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.{Subscriber, SubscriptionFsm, SubscriptionState}

  describe "acknowledgement sent before more than one reset" do
    test "covers only the delivery it was sent for" do
      event = %RecordedEvent{event_number: 1}

      subscriber =
        %Subscriber{}
        |> Subscriber.track_in_flight(event, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event, nil)

      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 1)
      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 1)
      assert {:ok, _subscriber, [^event]} = Subscriber.acknowledge(subscriber, 1)
    end

    test "is discarded for only the delivery it was sent for" do
      event = %RecordedEvent{event_number: 1}

      subscriber =
        %Subscriber{}
        |> Subscriber.track_in_flight(event, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.discard_superseded(1)
        |> Subscriber.track_in_flight(event, nil)

      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 1)
      assert {:ok, _subscriber, [^event]} = Subscriber.acknowledge(subscriber, 1)
    end

    test "keeps the deliveries newer than the one acknowledged" do
      [event1, event2] = Enum.map(1..2, &%RecordedEvent{event_number: &1})

      subscriber =
        %Subscriber{}
        |> Subscriber.track_in_flight(event1, nil)
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event1, nil)
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event1, nil)
        |> Subscriber.track_in_flight(event2, nil)

      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 2)
      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 1)
      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 2)
      assert {:ok, _subscriber, [^event1]} = Subscriber.acknowledge(subscriber, 1)
    end

    test "keeps the order the deliveries were sent in across resets of different events" do
      [event1, event2] = Enum.map(1..2, &%RecordedEvent{event_number: &1})

      subscriber =
        %Subscriber{}
        |> Subscriber.track_in_flight(event1, nil)
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event1, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event1, nil)

      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 1)
      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 2)
      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 1)
      assert {:ok, _subscriber, [^event1]} = Subscriber.acknowledge(subscriber, 1)
    end

    test "is discarded through the subscription for only the delivery it was sent for" do
      event = %RecordedEvent{event_number: 1}
      pid = self()

      subscriber =
        %Subscriber{pid: pid}
        |> Subscriber.track_in_flight(event, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event, nil)

      fsm = %SubscriptionFsm{
        state: :disconnected,
        data: %SubscriptionState{subscribers: %{pid => subscriber}}
      }

      %SubscriptionFsm{data: %SubscriptionState{subscribers: %{^pid => subscriber}}} =
        SubscriptionFsm.ack(fsm, 1, pid)

      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 1)
      assert {:ok, _subscriber, [^event]} = Subscriber.acknowledge(subscriber, 1)
    end

    test "is discarded through the subscription along with the deliveries it went past" do
      [event1, event2] = Enum.map(1..2, &%RecordedEvent{event_number: &1})
      pid = self()

      subscriber =
        %Subscriber{pid: pid}
        |> Subscriber.track_in_flight(event1, nil)
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event1, nil)

      fsm = %SubscriptionFsm{
        state: :disconnected,
        data: %SubscriptionState{subscribers: %{pid => subscriber}}
      }

      %SubscriptionFsm{data: %SubscriptionState{subscribers: %{^pid => subscriber}}} =
        SubscriptionFsm.ack(fsm, 2, pid)

      assert {:ok, _subscriber, [^event1]} = Subscriber.acknowledge(subscriber, 1)
    end
  end

  describe "acknowledgement of an event sent after a reset" do
    test "accounts for every earlier delivery the subscriber went past" do
      [event2, event3, event4] = Enum.map(2..4, &%RecordedEvent{event_number: &1})

      subscriber =
        %Subscriber{}
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.track_in_flight(event3, nil)
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.track_in_flight(event3, nil)
        |> Subscriber.track_in_flight(event4, nil)

      assert {:ok, subscriber, []} = Subscriber.acknowledge(subscriber, 3)

      assert {:ok, subscriber, [^event4, ^event3, ^event2]} =
               Subscriber.acknowledge(subscriber, 4)

      # Resuming from a checkpoint that had not caught up sends the acknowledged events again.
      subscriber =
        subscriber
        |> Subscriber.reset_in_flight()
        |> Subscriber.track_in_flight(event2, nil)
        |> Subscriber.track_in_flight(event3, nil)

      assert {:ok, _subscriber, [^event3, ^event2]} = Subscriber.acknowledge(subscriber, 3)
    end
  end
end
