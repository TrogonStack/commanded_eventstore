defmodule EventStore.Notifications.PublisherTest do
  use EventStore.StorageCase

  import EventStore.PublisherHelpers

  alias EventStore.{PubSub, UUID, Wait}
  alias EventStore.Notifications.Publisher
  alias TestEventStore, as: EventStore

  @publisher Module.concat([TestEventStore, Publisher])
  @deleting_store :publisher_deleting_store
  @deleting_publisher Module.concat([@deleting_store, Publisher])

  setup do
    wait_until_publishing()
  end

  describe "publish events" do
    test "appended concurrently to many streams" do
      subscribed_streams = for _i <- 1..6, do: UUID.uuid4()
      unsubscribed_stream = UUID.uuid4()
      streams = [unsubscribed_stream | subscribed_streams]

      all_collector = start_collector("$all")
      stream_collectors = Map.new(subscribed_streams, &{&1, start_collector(&1)})

      # Append 25 batches of one, two, or three events to each stream concurrently.
      appends =
        Map.new(streams, fn stream_uuid ->
          batches =
            for batch <- 1..25 do
              for n <- 1..(rem(batch, 3) + 1), do: "#{batch}.#{n}"
            end

          {stream_uuid, batches ++ [["last"]]}
        end)

      appends
      |> Task.async_stream(
        fn {stream_uuid, batches} -> Enum.each(batches, &append(stream_uuid, &1)) end,
        max_concurrency: map_size(appends),
        timeout: 60_000
      )
      |> Stream.run()

      expected =
        Map.new(appends, fn {stream_uuid, batches} -> {stream_uuid, List.flatten(batches)} end)

      all_events = await_last(all_collector, streams)

      assert_ordered_by_event_number(all_events)

      for {stream_uuid, labels} <- expected do
        stream_events = Enum.filter(all_events, &(&1.stream_uuid == stream_uuid))

        assert labels(stream_events) == labels
        assert stream_versions(stream_events) == Enum.to_list(1..length(labels))
      end

      for stream_uuid <- subscribed_streams do
        labels = Map.fetch!(expected, stream_uuid)
        stream_events = await_last(stream_collectors[stream_uuid], [stream_uuid])

        assert Enum.uniq(Enum.map(stream_events, & &1.stream_uuid)) == [stream_uuid]
        assert labels(stream_events) == labels
        assert event_numbers(stream_events) == Enum.to_list(1..length(labels))
        assert stream_versions(stream_events) == Enum.to_list(1..length(labels))
      end
    end

    test "appended in a single batch larger than the read ahead" do
      stream_uuid = UUID.uuid4()

      all_collector = start_collector("$all")
      stream_collector = start_collector(stream_uuid)

      labels = Enum.map(1..2_300, &to_string/1)

      append(stream_uuid, labels)

      Wait.until(10_000, fn ->
        assert length(received_events(stream_collector)) == length(labels)
        assert length(received_events(all_collector, stream_uuid)) == length(labels)
      end)

      # Append another event to ensure no duplicate events are received
      append(stream_uuid, ["last"])

      labels = labels ++ ["last"]

      stream_events = await_last(stream_collector, [stream_uuid])

      assert labels(stream_events) == labels
      assert event_numbers(stream_events) == Enum.to_list(1..length(labels))

      all_events = await_last(all_collector, [stream_uuid])

      assert_ordered_by_event_number(all_events)
      assert labels(all_events) == labels
      assert stream_versions(all_events) == Enum.to_list(1..length(labels))
    end

    # The first notification for the subscribed stream causes the `$all` stream to be read ahead,
    # ending part way through the large batch appended to the unsubscribed stream.
    test "appended while the publisher is suspended" do
      unsubscribed_stream = UUID.uuid4()
      subscribed_stream = UUID.uuid4()

      all_collector = start_collector("$all")
      stream_collector = start_collector(subscribed_stream)

      batch = Enum.map(1..1_500, &to_string/1)

      expected = %{
        unsubscribed_stream => batch ++ ["last"],
        subscribed_stream => ["1", "2", "3", "last"]
      }

      :ok = :sys.suspend(@publisher)

      append(subscribed_stream, ["1"])
      append(unsubscribed_stream, batch)
      append(subscribed_stream, ["2", "3"])
      append(unsubscribed_stream, ["last"])
      append(subscribed_stream, ["last"])

      :ok = :sys.resume(@publisher)

      all_events = await_last(all_collector, Map.keys(expected))

      assert_ordered_by_event_number(all_events)

      for {stream_uuid, labels} <- expected do
        assert all_events |> Enum.filter(&(&1.stream_uuid == stream_uuid)) |> labels() == labels
      end

      stream_events = await_last(stream_collector, [subscribed_stream])

      assert labels(stream_events) == expected[subscribed_stream]
      assert event_numbers(stream_events) == [1, 2, 3, 4]
    end

    test "to a stream subscribed after events have been appended" do
      stream_uuid = UUID.uuid4()

      all_collector = start_collector("$all")

      append(stream_uuid, ["1", "2", "3"])

      await_last(all_collector, [stream_uuid], "3")

      stream_collector = start_collector(stream_uuid)

      append(stream_uuid, ["4", "5"])
      append(stream_uuid, ["6"])

      stream_events = await_last(stream_collector, [stream_uuid], "6")

      assert Enum.map(stream_events, &{&1.event_number, &1.stream_version, label(&1)}) == [
               {4, 4, "4"},
               {5, 5, "5"},
               {6, 6, "6"}
             ]
    end

    # A persistent subscription which has caught up ignores a broadcast starting with an event it
    # has already seen, so each append must be published in its own broadcast.
    test "to a persistent subscription which caught up before the publisher published them" do
      stream_uuid = UUID.uuid4()
      subscription_name = UUID.uuid4()

      :ok = :sys.suspend(@publisher)

      append(stream_uuid, ["1", "2", "3"])

      {:ok, subscription} =
        EventStore.subscribe_to_all_streams(subscription_name, self(), start_from: :current)

      assert_receive {:subscribed, ^subscription}

      Wait.until(fn ->
        assert %{subscription: %{state: :subscribed}} = :sys.get_state(subscription)
      end)

      append(stream_uuid, ["4", "5", "6"])

      :ok = :sys.resume(@publisher)

      events = receive_and_ack(subscription, 3)

      refute_receive {:events, _events}

      assert labels(events) == ["4", "5", "6"]
      assert stream_versions(events) == [4, 5, 6]

      :ok = EventStore.unsubscribe_from_all_streams(subscription_name)
    end
  end

  describe "publish events when the recent reads do not cover the notification" do
    test "with the last read position ahead of every notification" do
      stream_uuid = UUID.uuid4()
      collectors = start_collectors(stream_uuid)

      replace_reads(&%{&1 | last_all_stream_version: &1.last_all_stream_version + 10_000})

      batches = [["1"], ["2", "3"], ["4"], ["last"]]
      Enum.each(batches, &append(stream_uuid, &1))

      assert_published(stream_uuid, batches, collectors)
    end

    test "with the last read position behind the start of the notification" do
      stream_uuid = UUID.uuid4()
      collectors = start_collectors(stream_uuid)

      replace_reads(&%{&1 | last_all_stream_version: 0})

      batches = [["1", "2"], ["3"], ["last"]]
      Enum.each(batches, &append(stream_uuid, &1))

      assert_published(stream_uuid, batches, collectors)
      assert_read_ahead_healed()
    end

    test "with the last read position inside the range of the notification" do
      stream_uuid = UUID.uuid4()
      collectors = start_collectors(stream_uuid)

      %{last_all_stream_version: last} = :sys.get_state(@publisher).state

      :ok = :sys.suspend(@publisher)

      batches = [["1", "2", "3"], ["4"], ["last"]]
      Enum.each(batches, &append(stream_uuid, &1))

      replace_reads(&%{&1 | last_all_stream_version: last + 2})

      :ok = :sys.resume(@publisher)

      assert_published(stream_uuid, batches, collectors)
      assert_read_ahead_healed()
    end

    test "after the recent reads are discarded between notifications" do
      stream_uuid = UUID.uuid4()
      collectors = start_collectors(stream_uuid)

      append(stream_uuid, ["1"])
      await_last(elem(collectors, 0), [stream_uuid], "1")

      replace_reads(& &1)

      append(stream_uuid, ["2", "3"])
      append(stream_uuid, ["last"])

      assert_published(stream_uuid, [["1"], ["2", "3"], ["last"]], collectors)
      assert_read_ahead_healed()
    end
  end

  describe "publish events appended to linked streams" do
    test "to the target stream numbered by the target stream version" do
      source = UUID.uuid4()
      target = UUID.uuid4()

      all_collector = start_collector("$all")
      source_collector = start_collector(source)
      target_collector = start_collector(target)

      append(source, ["a", "b", "last"])
      append(target, ["t1"])

      await_last(target_collector, [target], "t1")

      {:ok, source_events} = EventStore.read_stream_forward(source)
      :ok = EventStore.link_to_stream(target, 1, Enum.take(source_events, 2))

      Wait.until(10_000, fn -> assert length(received_batches(target_collector)) == 2 end)

      :sys.get_state(@publisher).state

      assert target_collector |> received_batches() |> Enum.map(&labels/1) == [["t1"], ["a", "b"]]

      [_own, linked] = received_batches(target_collector)

      assert event_numbers(linked) == [2, 3]
      assert Enum.map(linked, & &1.stream_uuid) == [source, source]
      assert stream_versions(linked) == [1, 2]

      assert source_collector |> received_batches() |> Enum.map(&labels/1) == [["a", "b", "last"]]

      assert all_collector |> received_batches() |> Enum.map(&labels/1) == [
               ["a", "b", "last"],
               ["t1"]
             ]
    end

    test "interleaved with the target stream's own events while the publisher is suspended" do
      source = UUID.uuid4()
      target = UUID.uuid4()

      all_collector = start_collector("$all")
      target_collector = start_collector(target)

      append(source, ["a", "b", "c"])

      {:ok, source_events} = EventStore.read_stream_forward(source)

      :ok = :sys.suspend(@publisher)

      append(target, ["t1"])
      :ok = EventStore.link_to_stream(target, 1, Enum.take(source_events, 2))
      append(target, ["t2"])
      append(target, ["last"])

      :ok = :sys.resume(@publisher)

      await_last(target_collector, [target])

      :sys.get_state(@publisher).state

      batches = received_batches(target_collector)

      assert Enum.map(batches, &labels/1) == [["t1"], ["a", "b"], ["t2"], ["last"]]
      assert Enum.map(batches, &event_numbers/1) == [[1], [2, 3], [4], [5]]

      all_events = received_events(all_collector)

      assert labels(all_events) == ["a", "b", "c", "t1", "t2", "last"]
      assert_ordered_by_event_number(all_events)
    end
  end

  describe "publish events to a persistent subscription to a stream" do
    test "which caught up before the publisher published them" do
      stream_uuid = UUID.uuid4()
      other_stream = UUID.uuid4()
      subscription_name = UUID.uuid4()

      :ok = :sys.suspend(@publisher)

      append(other_stream, ["x", "y"])
      append(stream_uuid, ["1", "2", "3"])

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(),
          start_from: :current
        )

      assert_receive {:subscribed, ^subscription}

      Wait.until(fn ->
        assert %{subscription: %{state: :subscribed}} = :sys.get_state(subscription)
      end)

      append(other_stream, ["z"])
      append(stream_uuid, ["4", "5", "6"])

      :ok = :sys.resume(@publisher)

      events = receive_and_ack(subscription, 3)

      refute_receive {:events, _events}

      assert labels(events) == ["4", "5", "6"]
      assert event_numbers(events) == [4, 5, 6]
      assert stream_versions(events) == [4, 5, 6]
      assert Enum.uniq(Enum.map(events, & &1.stream_uuid)) == [stream_uuid]

      :ok = EventStore.unsubscribe_from_stream(stream_uuid, subscription_name)
    end

    test "subscribed from the origin while appends continue" do
      stream_uuid = UUID.uuid4()
      other_stream = UUID.uuid4()
      subscription_name = UUID.uuid4()
      test = self()

      appender =
        Task.async(fn ->
          for n <- 1..40 do
            append(other_stream, ["o#{n}"])
            append(stream_uuid, ["#{n}.1", "#{n}.2"])

            send(test, {:appended, n})
          end

          append(stream_uuid, ["last"])
        end)

      assert_receive {:appended, 10}

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(),
          start_from: :origin
        )

      assert_receive {:subscribed, ^subscription}

      events = receive_and_ack_until(subscription, "last")

      Task.await(appender)

      refute_receive {:events, _events}

      expected = Enum.flat_map(1..40, &["#{&1}.1", "#{&1}.2"]) ++ ["last"]

      assert labels(events) == expected
      assert event_numbers(events) == Enum.to_list(1..length(expected))
      assert stream_versions(events) == Enum.to_list(1..length(expected))

      :ok = EventStore.unsubscribe_from_stream(stream_uuid, subscription_name)
    end
  end

  describe "publish events when a stream is deleted" do
    setup do
      start_supervised!({TestEventStore, name: @deleting_store, enable_hard_deletes: true})

      wait_until_publishing(@deleting_store, TestEventStore)
    end

    for type <- [:soft, :hard] do
      test "#{type} while its events are cached" do
        deleted = UUID.uuid4()
        stream_uuid = UUID.uuid4()
        publisher = Process.whereis(@deleting_publisher)

        all_collector = start_collector("$all", @deleting_store)
        deleted_collector = start_collector(deleted, @deleting_store)
        stream_collector = start_collector(stream_uuid, @deleting_store)

        append(deleted, ["d1", "d2"])
        append(stream_uuid, ["1"])

        await_last(stream_collector, [stream_uuid], "1")

        :ok =
          EventStore.delete_stream(deleted, :any_version, unquote(type), name: @deleting_store)

        append(stream_uuid, ["2", "3"])
        append(stream_uuid, ["last"])

        await_last(stream_collector, [stream_uuid])

        :sys.get_state(@deleting_publisher).state

        assert Process.whereis(@deleting_publisher) == publisher

        assert deleted_collector |> received_batches() |> Enum.map(&labels/1) == [["d1", "d2"]]

        assert stream_collector |> received_batches() |> Enum.map(&labels/1) ==
                 [["1"], ["2", "3"], ["last"]]

        all_events = received_events(all_collector)

        assert labels(all_events) == ["d1", "d2", "1", "2", "3", "last"]
        assert_ordered_by_event_number(all_events)

        assert Enum.all?(received_batches(all_collector), &(&1 != []))
      end
    end

    test "hard before the publisher read its events" do
      %{stream_collector: stream_collector, deleted_collector: deleted_collector} =
        hard_delete_before_publishing()

      assert deleted_collector |> received_events() |> labels() == []

      assert stream_collector |> received_batches() |> Enum.map(&labels/1) ==
               [["1", "2"], ["3"], ["last"]]
    end

    # A range of `$all` which contained a hard deleted event is read with a limit, so the read
    # which stands in for it returns the events following the range, as it did before the `$all`
    # stream was read ahead, and those events are then published a second time.
    @tag :skip
    test "hard before the publisher read its events, publishes each `$all` event once" do
      %{all_collector: all_collector} = hard_delete_before_publishing()

      all_events = received_events(all_collector)

      assert labels(all_events) == ["1", "2", "3", "last"]
      assert_ordered_by_event_number(all_events)
    end
  end

  defp hard_delete_before_publishing do
    deleted = UUID.uuid4()
    stream_uuid = UUID.uuid4()

    all_collector = start_collector("$all", @deleting_store)
    deleted_collector = start_collector(deleted, @deleting_store)
    stream_collector = start_collector(stream_uuid, @deleting_store)

    :ok = :sys.suspend(@deleting_publisher)

    append(deleted, ["d1", "d2"])
    append(stream_uuid, ["1", "2"])

    :ok = EventStore.delete_stream(deleted, :any_version, :hard, name: @deleting_store)

    append(stream_uuid, ["3"])
    append(stream_uuid, ["last"])

    :ok = :sys.resume(@deleting_publisher)

    await_last(stream_collector, [stream_uuid])

    :sys.get_state(@deleting_publisher)

    %{
      all_collector: all_collector,
      deleted_collector: deleted_collector,
      stream_collector: stream_collector
    }
  end

  describe "with two event stores" do
    setup do
      reset_event_store!(SecondEventStore)
      start_supervised!({SecondEventStore, name: SecondEventStore})

      wait_until_publishing(SecondEventStore)
    end

    test "subscribers are only notified of the events of their own event store" do
      shared = UUID.uuid4()
      first_only = UUID.uuid4()
      second_only = UUID.uuid4()

      first_all = start_collector("$all", TestEventStore)
      second_all = start_collector("$all", SecondEventStore)
      first_shared = start_collector(shared, TestEventStore)
      second_shared = start_collector(shared, SecondEventStore)
      first_first_only = start_collector(first_only, TestEventStore)
      first_second_only = start_collector(second_only, TestEventStore)

      append(shared, ["f1", "f2"], TestEventStore)
      append(shared, ["s1"], SecondEventStore)
      append(first_only, ["fo", "last"], TestEventStore)
      append(second_only, ["so", "last"], SecondEventStore)
      append(shared, ["last"], TestEventStore)
      append(shared, ["last"], SecondEventStore)

      all_first = await_last(first_all, [shared, first_only])
      all_second = await_last(second_all, [shared, second_only])

      assert all_first |> Enum.filter(&(&1.stream_uuid == shared)) |> labels() ==
               ["f1", "f2", "last"]

      assert all_second |> Enum.filter(&(&1.stream_uuid == shared)) |> labels() == ["s1", "last"]
      assert Enum.all?(all_first, &(&1.stream_uuid in [shared, first_only]))
      assert Enum.all?(all_second, &(&1.stream_uuid in [shared, second_only]))
      assert_ordered_by_event_number(all_first)
      assert_ordered_by_event_number(all_second)

      assert first_shared |> await_last([shared]) |> labels() == ["f1", "f2", "last"]
      assert second_shared |> await_last([shared]) |> labels() == ["s1", "last"]
      assert first_first_only |> await_last([first_only]) |> labels() == ["fo", "last"]

      :sys.get_state(@publisher).state
      :sys.get_state(second_publisher()).state

      assert received_events(first_second_only) == []
    end

    test "a stream subscribed in one event store is not published in the other" do
      stream_uuid = UUID.uuid4()

      assert PubSub.subscribed?(TestEventStore, stream_uuid) == false

      first_collector = start_collector(stream_uuid, TestEventStore)

      assert PubSub.subscribed?(TestEventStore, stream_uuid) == true
      assert PubSub.subscribed?(SecondEventStore, stream_uuid) == false

      append(stream_uuid, ["second"], SecondEventStore)

      :sys.get_state(second_publisher()).state

      assert received_events(first_collector) == []

      append(stream_uuid, ["first", "last"], TestEventStore)

      assert first_collector |> await_last([stream_uuid]) |> labels() == ["first", "last"]

      assert Enum.all?(
               Tuple.to_list(:sys.get_state(@publisher).state.recent_reads),
               fn reads ->
                 reads |> Map.get(stream_uuid, []) |> labels() |> Enum.all?(&(&1 != "second"))
               end
             )

      second_collector = start_collector(stream_uuid, SecondEventStore)

      append(stream_uuid, ["next", "last"], SecondEventStore)

      second_events = await_last(second_collector, [stream_uuid])

      assert labels(second_events) == ["next", "last"]
      assert stream_versions(second_events) == [2, 3]
    end
  end

  defp second_publisher, do: Module.concat([SecondEventStore, Publisher])

  # Start collectors for the `$all` stream and the stream.
  defp start_collectors(stream_uuid),
    do: {start_collector("$all"), start_collector(stream_uuid)}

  defp replace_reads(fun),
    do: :sys.replace_state(@publisher, fn stage -> %{stage | state: fun.(stage.state)} end)

  # Every notification is published exactly once as its own broadcast of exactly its range, in
  # the order of the appends, numbered by position for `$all` and by version for the stream.
  defp assert_published(stream_uuid, batches, {all_collector, stream_collector}) do
    await_last(all_collector, [stream_uuid])
    await_last(stream_collector, [stream_uuid])

    :sys.get_state(@publisher).state

    stream_batches = received_batches(stream_collector)
    all_batches = received_batches(all_collector)

    assert Enum.map(stream_batches, &labels/1) == batches
    assert Enum.map(all_batches, &labels/1) == batches

    {versions, _count} =
      Enum.map_reduce(batches, 0, fn batch, count ->
        {Enum.to_list((count + 1)..(count + length(batch))), count + length(batch)}
      end)

    assert Enum.map(stream_batches, &stream_versions/1) == versions
    assert Enum.map(stream_batches, &event_numbers/1) == versions
    assert Enum.map(all_batches, &stream_versions/1) == versions

    numbers = all_batches |> Enum.concat() |> event_numbers()

    assert numbers == Enum.to_list(hd(numbers)..List.last(numbers))
  end

  # After a notification is served by a read, the next one is served from the events read ahead.
  defp assert_read_ahead_healed do
    {:ok, [_ | _] = events} = EventStore.read_all_streams_backward(-1, 1)
    last = hd(events).event_number

    assert %{last_all_stream_version: ^last, recent_reads: {%{"$all" => cached}, _previous}} =
             :sys.get_state(@publisher).state

    assert List.last(cached).event_number == last
  end
end
