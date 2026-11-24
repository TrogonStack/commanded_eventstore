defmodule EventStore.Notifications.PublisherFaultTest do
  use EventStore.StorageCase

  import EventStore.PublisherHelpers

  alias EventStore.{ListenerHelpers, TcpProxy, UUID, Wait}
  alias EventStore.Notifications.Publisher
  alias TestEventStore, as: EventStore

  @publisher Module.concat([TestEventStore, Publisher])

  describe "the publisher is killed" do
    setup do
      wait_until_publishing()
    end

    # Notifications sent while the publisher restarts are lost, so the persistent subscriptions
    # have to catch up from storage once the next notification arrives.
    test "while appending to a stream with persistent and transient subscribers" do
      stream_uuid = UUID.uuid4()
      other_stream = UUID.uuid4()
      test = self()

      all_collector = start_collector("$all")
      stream_collector = start_collector(stream_uuid)

      all_subscriber =
        start_persistent_subscriber(fn subscriber ->
          EventStore.subscribe_to_all_streams(UUID.uuid4(), subscriber, start_from: :origin)
        end)

      stream_subscriber =
        start_persistent_subscriber(fn subscriber ->
          EventStore.subscribe_to_stream(stream_uuid, UUID.uuid4(), subscriber,
            start_from: :origin
          )
        end)

      appender =
        Task.async(fn ->
          for n <- 1..30 do
            append(stream_uuid, ["#{n}.1", "#{n}.2"])
            append(other_stream, ["o#{n}"])

            if n == 10 do
              send(test, :appended)

              receive do: (:killed -> :ok)
            end
          end
        end)

      assert_receive :appended

      publisher = Process.whereis(@publisher)
      ref = Process.monitor(publisher)

      Process.exit(publisher, :kill)
      send(appender.pid, :killed)

      assert_receive {:DOWN, ^ref, :process, ^publisher, :killed}

      Task.await(appender)

      Wait.until(10_000, fn ->
        assert is_pid(Process.whereis(@publisher))
        refute Process.whereis(@publisher) == publisher
      end)

      # Keep appending until a notification is published by the restarted publisher.
      Wait.until(10_000, fn ->
        append(stream_uuid, ["tick"])

        assert Enum.any?(received_events(stream_subscriber), &(label(&1) == "tick"))
      end)

      {:ok, all_stored} = EventStore.read_all_streams_forward(0, 10_000)
      {:ok, stream_stored} = EventStore.read_stream_forward(stream_uuid, 0, 10_000)

      last = List.last(all_stored).event_number

      Wait.until(10_000, fn ->
        assert List.last(received_events(all_subscriber)).event_number == last
      end)

      all_events = received_events(all_subscriber)

      assert event_numbers(all_events) == Enum.to_list(1..last)
      assert labels(all_events) == labels(all_stored)

      stream_events = received_events(stream_subscriber)

      assert labels(stream_events) == labels(stream_stored)
      assert event_numbers(stream_events) == Enum.to_list(1..length(stream_stored))
      assert stream_versions(stream_events) == Enum.to_list(1..length(stream_stored))

      for collector <- [all_collector, stream_collector] do
        Wait.until(10_000, fn ->
          assert collector |> received_events() |> Enum.any?(&(label(&1) == "tick"))
        end)

        events = received_events(collector)

        assert_ordered_by_event_number(events)
      end
    end
  end

  describe "the connection the publisher reads through fails" do
    @store :publisher_fault_store
    @store_publisher Module.concat([@store, Publisher])
    @query_timeout 500

    setup %{conn: conn, config: config} do
      port = Keyword.fetch!(config, :port)
      application_name = ListenerHelpers.unique_application_name("publisher_fault")

      proxy = start_supervised!({TcpProxy, {Keyword.fetch!(config, :hostname), port}})

      start_supervised!(
        {TestEventStore,
         name: @store,
         port: TcpProxy.port(proxy),
         session_mode_pool: [port: port, parameters: [application_name: application_name]],
         idle_interval: 60_000,
         timeout: @query_timeout}
      )

      ListenerHelpers.await_listening(conn, application_name)

      wait_until_publishing(@store, TestEventStore)

      handler = :"publisher_fault_#{System.unique_integer([:positive])}"
      :ok = :logger.add_handler(handler, __MODULE__, %{config: %{test: self()}})
      on_exit(fn -> :logger.remove_handler(handler) end)

      [proxy: proxy]
    end

    # Handler which reports the publisher's failed reads to the test.
    def log(%{msg: {:string, message}}, %{config: %{test: test}}) do
      if message |> IO.chardata_to_string() |> String.contains?("notifications failed to read") do
        send(test, :read_failed)
      end

      :ok
    end

    def log(_event, _config), do: :ok

    test "publishes the events appended after the failure from the right position", %{
      proxy: proxy
    } do
      stream_uuid = UUID.uuid4()

      all_collector = start_collector("$all", @store)

      append(stream_uuid, ["1"])
      await_last(all_collector, [stream_uuid], "1")

      %{last_all_stream_version: before_failure} = :sys.get_state(@store_publisher).state

      :ok = TcpProxy.freeze(proxy)

      append(stream_uuid, ["lost"])

      # The read ahead and then the read of the notification's own range both fail.
      assert_receive :read_failed, 10_000
      assert_receive :read_failed, 10_000

      assert %{last_all_stream_version: ^before_failure} = :sys.get_state(@store_publisher).state

      :ok = TcpProxy.forward(proxy)

      Wait.until(10_000, fn ->
        assert {:ok, _events} = EventStore.read_all_streams_forward(0, 1, name: @store)
      end)

      stream_collector = start_collector(stream_uuid, @store)

      append(stream_uuid, ["5", "6"])
      append(stream_uuid, ["7"])
      append(stream_uuid, ["last"])

      await_last(all_collector, [stream_uuid])
      await_last(stream_collector, [stream_uuid])

      :sys.get_state(@store_publisher)

      all_batches = received_batches(all_collector)
      stream_batches = received_batches(stream_collector)

      assert Enum.map(all_batches, &labels/1) == [["1"], ["5", "6"], ["7"], ["last"]]
      assert Enum.map(stream_batches, &labels/1) == [["5", "6"], ["7"], ["last"]]

      {:ok, stored} = EventStore.read_all_streams_forward(0, 10_000)
      stored = Enum.filter(stored, &(&1.stream_uuid == stream_uuid))

      assert all_batches |> Enum.drop(1) |> Enum.concat() |> event_numbers() ==
               stored |> Enum.drop(2) |> event_numbers()

      assert stream_batches |> Enum.concat() |> event_numbers() == [3, 4, 5, 6]
      assert stream_batches |> Enum.concat() |> stream_versions() == [3, 4, 5, 6]

      %{last_all_stream_version: last, recent_reads: {%{"$all" => cached}, _previous}} =
        :sys.get_state(@store_publisher).state

      assert last == List.last(stored).event_number
      assert List.last(cached).event_number == last
    end
  end
end
