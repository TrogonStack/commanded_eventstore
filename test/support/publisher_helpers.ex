defmodule EventStore.PublisherHelpers do
  @moduledoc false

  import ExUnit.Assertions

  alias EventStore.{Config, EventData, EventFactory, PubSub, RecordedEvent, Storage, UUID, Wait}

  # Postgres does not queue notifications for a `LISTEN` which has not yet been established, so
  # append events until they are published. The first notification is published before the
  # `$all` stream has been read.
  def wait_until_publishing(event_store \\ TestEventStore, append_with \\ nil) do
    append_with = append_with || event_store
    stream_uuid = UUID.uuid4()

    all_collector = start_collector("$all", event_store)
    stream_collector = start_collector(stream_uuid, event_store)

    Wait.until(5_000, fn ->
      append(stream_uuid, ["probe"], append_with)

      Wait.until(100, fn -> assert [_ | _] = received_events(all_collector, stream_uuid) end)
    end)

    append(stream_uuid, ["last"], append_with)

    all_events = await_last(all_collector, [stream_uuid])
    stream_events = await_last(stream_collector, [stream_uuid])

    versions = stream_versions(all_events)

    assert versions == Enum.to_list(hd(versions)..List.last(versions))
    assert event_numbers(stream_events) == versions
    assert stream_versions(stream_events) == versions

    stop_collector(all_collector)
    stop_collector(stream_collector)

    :ok
  end

  # Receive and acknowledge events from the subscription until the expected number are received.
  def receive_and_ack(subscription, count, received \\ [], event_store \\ TestEventStore)

  def receive_and_ack(_subscription, count, received, _event_store)
      when length(received) >= count,
      do: received

  def receive_and_ack(subscription, count, received, event_store) do
    assert_receive {:events, events}

    :ok = event_store.ack(subscription, events)

    receive_and_ack(subscription, count, received ++ events, event_store)
  end

  # Receive and acknowledge events from the subscription until one with the label is received.
  def receive_and_ack_until(subscription, label, received \\ [], event_store \\ TestEventStore) do
    if Enum.any?(received, &(label(&1) == label)) do
      received
    else
      assert_receive {:events, events}, 10_000

      :ok = event_store.ack(subscription, events)

      receive_and_ack_until(subscription, label, received ++ events, event_store)
    end
  end

  def append(stream_uuid, labels, event_store \\ TestEventStore) do
    events =
      Enum.map(labels, fn label ->
        %EventData{
          event_type: "Elixir.EventStore.EventFactory.Event",
          data: %EventFactory.Event{event: label},
          metadata: %{}
        }
      end)

    :ok = event_store.append_to_stream(stream_uuid, :any_version, events)
  end

  # Wait until the last event of each given stream has been received.
  def await_last(collector, stream_uuids, last_label \\ "last") do
    Wait.until(10_000, fn ->
      events = received_events(collector)

      for stream_uuid <- stream_uuids do
        assert Enum.any?(events, &(&1.stream_uuid == stream_uuid and label(&1) == last_label))
      end

      Enum.filter(events, &(&1.stream_uuid in stream_uuids))
    end)
  end

  def assert_ordered_by_event_number(events) do
    event_numbers = event_numbers(events)

    assert event_numbers == Enum.sort(event_numbers)
    assert event_numbers == Enum.uniq(event_numbers)
  end

  def label(%RecordedEvent{data: %EventFactory.Event{event: label}}), do: label

  def labels(events), do: Enum.map(events, &label/1)
  def event_numbers(events), do: Enum.map(events, & &1.event_number)
  def stream_versions(events), do: Enum.map(events, & &1.stream_version)

  # Start a process subscribed to the topic which collects the events it receives.
  def start_collector(topic, event_store \\ TestEventStore) do
    reply_to = self()

    collector =
      spawn_link(fn ->
        :ok = PubSub.subscribe(event_store, topic)

        send(reply_to, {:subscribed, self()})

        collect([])
      end)

    assert_receive {:subscribed, ^collector}

    collector
  end

  defp collect(received) do
    receive do
      {:events, events} ->
        collect([events | received])

      {:received_events, reply_to} ->
        send(reply_to, {:received_events, self(), received |> Enum.reverse() |> Enum.concat()})

        collect(received)

      {:received_batches, reply_to} ->
        send(reply_to, {:received_batches, self(), Enum.reverse(received)})

        collect(received)
    end
  end

  def received_events(collector) do
    send(collector, {:received_events, self()})

    assert_receive {:received_events, ^collector, events}

    events
  end

  def received_events(collector, stream_uuid) do
    collector |> received_events() |> Enum.filter(&(&1.stream_uuid == stream_uuid))
  end

  # The events of each broadcast the collector received, in the order they were received.
  def received_batches(collector) do
    send(collector, {:received_batches, self()})

    assert_receive {:received_batches, ^collector, batches}

    batches
  end

  def stop_collector(collector) do
    Process.unlink(collector)
    Process.exit(collector, :kill)
  end

  # Start a process holding a persistent subscription which acknowledges and records every event
  # it receives. `subscribe` is called in the new process with the subscriber pid.
  def start_persistent_subscriber(subscribe, event_store \\ TestEventStore) do
    reply_to = self()

    subscriber =
      spawn_link(fn ->
        {:ok, subscription} = subscribe.(self())

        persistent_loop(subscription, event_store, reply_to, [])
      end)

    assert_receive {:subscribed, ^subscriber}

    subscriber
  end

  defp persistent_loop(subscription, event_store, reply_to, received) do
    receive do
      {:subscribed, ^subscription} ->
        send(reply_to, {:subscribed, self()})

        persistent_loop(subscription, event_store, reply_to, received)

      {:events, events} ->
        :ok = event_store.ack(subscription, events)

        persistent_loop(subscription, event_store, reply_to, [events | received])

      {:received_events, from} ->
        send(from, {:received_events, self(), received |> Enum.reverse() |> Enum.concat()})

        persistent_loop(subscription, event_store, reply_to, received)
    end
  end

  def reset_event_store!(event_store) do
    config = event_store.config()
    postgrex_config = Config.default_postgrex_opts(config)

    {:ok, conn} = Postgrex.start_link(postgrex_config)

    try do
      Storage.Initializer.reset!(conn, config)
    after
      GenServer.stop(conn)
    end
  end
end
