defmodule EventStore.Notifications.PublisherPropertyTest do
  use EventStore.StorageCase
  use ExUnitProperties

  import EventStore.PublisherHelpers

  alias EventStore.UUID

  @publisher Module.concat([TestEventStore, EventStore.Notifications.Publisher])

  setup do
    wait_until_publishing()
  end

  property "every topic receives each event once, in order, numbered as its stream numbers them" do
    check all(
            plan <- plan(),
            max_runs: 15
          ) do
      run(plan)
    end
  end

  defp plan do
    gen all(
          batches <- list_of(list_of(batch_size(), min_length: 1, max_length: 6), length: 2..5),
          subscribed <- list_of(boolean(), length: length(batches)),
          concurrency <- integer(1..5),
          suspended <- boolean()
        ) do
      %{batches: batches, subscribed: subscribed, concurrency: concurrency, suspended: suspended}
    end
  end

  defp batch_size, do: frequency([{9, integer(1..4)}, {1, integer(5..30)}])

  defp run(%{batches: batches, subscribed: subscribed, concurrency: concurrency} = plan) do
    streams =
      batches
      |> Enum.zip(subscribed)
      |> Enum.with_index()
      |> Enum.map(fn {{sizes, subscribed?}, index} ->
        labels =
          sizes
          |> Enum.with_index(1)
          |> Enum.map(fn {size, batch} -> for n <- 1..size, do: "#{index}.#{batch}.#{n}" end)

        %{uuid: UUID.uuid4(), batches: labels, subscribed?: subscribed?}
      end)

    all_collector = start_collector("$all")

    collectors =
      for %{subscribed?: true} = stream <- streams, do: {stream, start_collector(stream.uuid)}

    if plan.suspended, do: :ok = :sys.suspend(@publisher)

    try do
      streams
      |> Task.async_stream(
        fn stream -> Enum.each(stream.batches, &append(stream.uuid, &1)) end,
        max_concurrency: concurrency,
        timeout: 60_000
      )
      |> Stream.run()
    after
      if plan.suspended, do: :ok = :sys.resume(@publisher)
    end

    # The sentinels are appended after every other event so receiving them means the publisher
    # has handled every notification which could have published an event twice.
    Enum.each(streams, &append(&1.uuid, ["last"]))

    uuids = Enum.map(streams, & &1.uuid)

    await_last(all_collector, uuids)

    for {stream, collector} <- collectors, do: await_last(collector, [stream.uuid])

    :sys.get_state(@publisher)

    all_events = received_events(all_collector)

    try do
      assert_ordered_by_event_number(all_events)

      numbers = event_numbers(all_events)
      assert numbers == Enum.to_list(hd(numbers)..List.last(numbers))
      assert Enum.all?(all_events, &(&1.stream_uuid in uuids))

      for stream <- streams do
        expected = List.flatten(stream.batches) ++ ["last"]
        versions = Enum.to_list(1..length(expected))
        events = Enum.filter(all_events, &(&1.stream_uuid == stream.uuid))

        assert labels(events) == expected
        assert stream_versions(events) == versions
      end

      for {stream, collector} <- collectors do
        expected = List.flatten(stream.batches) ++ ["last"]
        versions = Enum.to_list(1..length(expected))
        events = received_events(collector)

        assert labels(events) == expected
        assert event_numbers(events) == versions
        assert stream_versions(events) == versions
        assert Enum.all?(events, &(&1.stream_uuid == stream.uuid))
      end
    after
      stop_collector(all_collector)
      Enum.each(collectors, fn {_stream, collector} -> stop_collector(collector) end)
    end
  end
end
