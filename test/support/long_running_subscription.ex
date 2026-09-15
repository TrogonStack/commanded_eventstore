defmodule EventStore.LongRunningSubscription do
  @moduledoc """
  Appends to a stream on an interval and logs everything a subscription receives.

      MIX_ENV=test mix run --no-halt -e "EventStore.LongRunningSubscription.start()"
  """

  alias EventStore.UUID

  defmodule ExampleEvent do
    @derive Jason.Encoder
    defstruct [:event]
  end

  defmodule LoggingSubscriber do
    use GenServer

    alias EventStore.UUID

    require Logger

    def start_link(stream_uuid) do
      GenServer.start_link(__MODULE__, stream_uuid)
    end

    @impl GenServer
    def init(stream_uuid) do
      {:ok, subscribe_to_stream(stream_uuid)}
    end

    @impl GenServer
    def handle_info({:subscribed, subscription}, subscription) do
      Logger.debug("Subscribed to stream")

      {:noreply, subscription}
    end

    @impl GenServer
    def handle_info({:events, events}, subscription) do
      Logger.debug("Received event(s): #{inspect(events)}")

      :ok = TestEventStore.ack(subscription, events)

      {:noreply, subscription}
    end

    defp subscribe_to_stream(stream_uuid) do
      {:ok, subscription} =
        TestEventStore.subscribe_to_stream(stream_uuid, UUID.uuid4(), self())

      subscription
    end
  end

  defmodule IntervalAppender do
    use GenServer

    alias EventStore.{EventData, UUID}
    alias EventStore.LongRunningSubscription.ExampleEvent

    def start_link(stream_uuid, expected_version \\ 0, interval \\ 30_000) do
      GenServer.start_link(__MODULE__, {stream_uuid, expected_version, interval})
    end

    @impl GenServer
    def init({stream_uuid, expected_version, interval}) do
      Process.send_after(self(), :append_to_stream, interval)

      {:ok, {stream_uuid, expected_version, interval}}
    end

    @impl GenServer
    def handle_info(:append_to_stream, {stream_uuid, expected_version, interval}) do
      events = [
        %EventData{
          correlation_id: UUID.uuid4(),
          causation_id: UUID.uuid4(),
          event_type: "Elixir.EventStore.LongRunningSubscription.ExampleEvent",
          data: %ExampleEvent{event: expected_version + 1},
          metadata: %{"user" => "user@example.com"}
        }
      ]

      :ok = TestEventStore.append_to_stream(stream_uuid, expected_version, events)

      Process.send_after(self(), :append_to_stream, interval)

      {:noreply, {stream_uuid, expected_version + 1, interval}}
    end
  end

  def start do
    {:ok, _pid} = TestEventStore.start_link()

    stream_uuid = UUID.uuid4()

    {:ok, _subscriber} = LoggingSubscriber.start_link("$all")
    {:ok, _subscriber} = LoggingSubscriber.start_link(stream_uuid)
    {:ok, _appender} = IntervalAppender.start_link(stream_uuid)

    :ok
  end
end
