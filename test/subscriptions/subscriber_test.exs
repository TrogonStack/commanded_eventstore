defmodule EventStore.Subscriptions.SubscriberTest do
  use ExUnit.Case, async: true

  alias EventStore.RecordedEvent
  alias EventStore.Subscriptions.Subscriber

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
  end
end
