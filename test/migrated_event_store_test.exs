defmodule EventStore.MigratedEventStoreTest do
  use ExUnit.Case

  alias EventStore.RecordedEvent
  alias EventStore.Snapshots.SnapshotData

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    recreate_database(config)
    migrate_eventstore(config)

    start_supervised!(TestEventStore)

    :ok
  end

  describe "migrated event store" do
    test "read stream forward" do
      {:ok, recorded_events} = TestEventStore.read_stream_forward("stream-1", 0)

      assert length(recorded_events) == 10
    end

    test "read linked stream forward" do
      {:ok, recorded_events} = TestEventStore.read_stream_forward("linked-stream", 0)

      assert length(recorded_events) == 10
    end

    test "resume subscription to all streams from origin" do
      {:ok, subscription} =
        TestEventStore.subscribe_to_all_streams("subscription-all-origin", self())

      assert_receive {:subscribed, ^subscription}

      for event_number <- 1..100 do
        assert_receive_event(subscription,
          expected_event_number: event_number,
          expected_stream_version:
            case Integer.mod(event_number, 10) do
              0 -> 10
              mod -> mod
            end
        )
      end

      refute_receive {:events, _received_events}
    end

    test "resume subscription to all streams from current" do
      {:ok, subscription} =
        TestEventStore.subscribe_to_all_streams("subscription-all-current", self())

      assert_receive {:subscribed, ^subscription}
      refute_receive {:events, _received_events}
    end

    test "resume subscription to single stream from origin" do
      {:ok, subscription} =
        TestEventStore.subscribe_to_stream("stream-1", "subscription-stream-1-origin", self())

      assert_receive {:subscribed, ^subscription}

      for stream_version <- 1..10 do
        assert_receive_event(subscription, expected_stream_version: stream_version)
      end

      refute_receive {:events, _received_events}
    end

    test "resume subscription to single stream from current" do
      {:ok, subscription} =
        TestEventStore.subscribe_to_stream("stream-1", "subscription-stream-1-current", self())

      assert_receive {:subscribed, ^subscription}
      refute_receive {:events, _received_events}
    end

    test "read a snapshot" do
      for i <- 1..10 do
        {:ok, snapshot} = TestEventStore.read_snapshot("snapshot-#{i}")

        expected_source_uuid = "snapshot-#{i}"

        assert match?(
                 %SnapshotData{
                   source_uuid: ^expected_source_uuid,
                   source_version: 1,
                   source_type: "Elixir.Snapshot",
                   data: %Snapshot{data: ^i}
                 },
                 snapshot
               )
      end
    end
  end

  defp assert_receive_event(subscription, opts) do
    assert_receive {:events, [%RecordedEvent{data: data} = received_event]}

    with {:ok, expected_event_number} <- Keyword.fetch(opts, :expected_event_number) do
      assert match?(%RecordedEvent{event_number: ^expected_event_number}, received_event)
    end

    with {:ok, expected_stream_version} <- Keyword.fetch(opts, :expected_stream_version) do
      assert match?(%RecordedEvent{stream_version: ^expected_stream_version}, received_event)
      assert match?(%Event{data: ^expected_stream_version, version: "1"}, data)
    end

    :ok = TestEventStore.ack(subscription, received_event)
  end

  defp recreate_database(config) do
    EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)

    {_, 0} = EventStore.Storage.Database.restore(config, "test/fixture/eventstore_seed.dump")
  end

  defp migrate_eventstore(config) do
    EventStore.Tasks.Migrate.exec(config, quiet: true)
  end
end

defmodule EventStore.MigratedSubscriptionLockTest do
  use ExUnit.Case

  alias EventStore.Config
  alias EventStore.Storage
  alias EventStore.Subscriptions.FencingToken

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    EventStore.Storage.Database.drop(config)
    :ok = EventStore.Storage.Database.create(config)
    {_, 0} = EventStore.Storage.Database.restore(config, "test/fixture/eventstore_seed.dump")
    EventStore.Tasks.Migrate.exec(config, quiet: true)

    postgrex_config =
      TestEventStore |> Config.parsed(:eventstore) |> Config.default_postgrex_opts()

    [conn: start_supervised!({Postgrex, postgrex_config})]
  end

  test "claims the lock of a subscription that existed before the migration", %{conn: conn} do
    assert [0, 0, 0, 0] == lock_generations(conn)

    assert {:ok, lease} = Storage.Lock.claim_subscription_lock(conn, 2, schema: "public")

    assert lease.last_seen == 100
    assert lease.token == FencingToken.new(2, 1)
    assert [0, 1, 0, 0] == lock_generations(conn)

    assert :ok =
             Storage.Subscription.ack_last_seen_event(conn, lease.token, 101, schema: "public")

    assert %Postgrex.Result{rows: [[101]]} =
             Postgrex.query!(
               conn,
               "SELECT last_seen FROM public.subscriptions WHERE subscription_id = 2",
               []
             )
  end

  defp lock_generations(conn) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT lock_generation FROM public.subscriptions ORDER BY subscription_id",
        []
      )

    List.flatten(rows)
  end
end
