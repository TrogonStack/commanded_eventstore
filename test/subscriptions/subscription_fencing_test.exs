defmodule EventStore.Subscriptions.SubscriptionFencingTest do
  use EventStore.StorageCase

  alias EventStore.{Config, EventFactory, TcpProxy, UUID, Wait}
  alias EventStore.Subscriptions.Subscription

  @store_a :fenced_store_a
  @store_b :fenced_store_b

  test "a subscription that lost its lock does not overwrite the progress of the new owner",
       %{conn: conn, schema: schema} do
    config = Config.parsed(TestEventStore, :eventstore)
    subscription_name = UUID.uuid4()

    proxy =
      start_supervised!(
        {TcpProxy, {Keyword.fetch!(config, :hostname), Keyword.get(config, :port, 5432)}}
      )

    try do
      start_supervised!(
        {TestEventStore,
         name: @store_a,
         schema: schema,
         session_mode_pool: [port: TcpProxy.port(proxy), timeout: 60_000, idle_interval: 100]},
        id: @store_a
      )

      start_supervised!({TestEventStore, name: @store_b, schema: schema}, id: @store_b)

      {:ok, subscription_a} =
        TestEventStore.subscribe_to_all_streams(subscription_name, self(),
          name: @store_a,
          buffer_size: 10
        )

      assert_receive {:subscribed, ^subscription_a}

      :ok =
        TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(7),
          name: @store_a
        )

      assert_receive_through(7)

      :ok = TestEventStore.ack(subscription_a, 3)

      Wait.until(fn -> assert 3 == last_seen(conn, schema, subscription_name) end)

      lock_pool = Module.concat([@store_a, EventStore.AdvisoryLocks, Postgrex])

      %Postgrex.Result{rows: [[backend_pid]]} =
        Postgrex.query!(lock_pool, "SELECT pg_backend_pid()", [])

      :ok = TcpProxy.blackhole(proxy)

      %Postgrex.Result{rows: [[true]]} =
        Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

      Wait.until(fn ->
        assert %Postgrex.Result{rows: [[0]]} =
                 Postgrex.query!(
                   conn,
                   "SELECT count(*)::int FROM pg_stat_activity WHERE pid = $1",
                   [backend_pid]
                 )
      end)

      subscriber_b = start_forwarding_subscriber()

      {:ok, subscription_b} =
        TestEventStore.subscribe_to_all_streams(subscription_name, subscriber_b,
          name: @store_b,
          buffer_size: 10
        )

      assert_receive {:b_subscribed, ^subscription_b}, 5_000
      assert_receive {:b_events, [%{event_number: 4} | _]}, 5_000

      send(subscriber_b, {:ack, subscription_b, 5})

      Wait.until(fn -> assert 5 == last_seen(conn, schema, subscription_name) end)

      flush_events()

      :ok = TestEventStore.ack(subscription_a, 7)

      assert 5 == last_seen(conn, schema, subscription_name)

      Wait.until(fn -> assert :disconnected == subscription_state(subscription_a) end)

      assert 5 == last_seen(conn, schema, subscription_name)

      refute_receive {:events, _events}, 500
    after
      TcpProxy.stop(proxy)
    end
  end

  defp assert_receive_through(last_event_number) do
    assert_receive {:events, events}

    unless List.last(events).event_number >= last_event_number do
      assert_receive_through(last_event_number)
    end
  end

  defp flush_events do
    receive do
      {:events, _events} -> flush_events()
    after
      0 -> :ok
    end
  end

  defp start_forwarding_subscriber do
    reply_to = self()

    spawn_link(fn -> forward(reply_to) end)
  end

  defp forward(reply_to) do
    receive do
      {:subscribed, subscription} -> send(reply_to, {:b_subscribed, subscription})
      {:events, events} -> send(reply_to, {:b_events, events})
      {:ack, subscription, event_number} -> TestEventStore.ack(subscription, event_number)
    end

    forward(reply_to)
  end

  defp subscription_state(subscription) do
    %Subscription{subscription: %{state: state}} = :sys.get_state(subscription)

    state
  end

  defp last_seen(conn, schema, subscription_name) do
    %Postgrex.Result{rows: [[last_seen]]} =
      Postgrex.query!(
        conn,
        "SELECT last_seen FROM \"#{schema}\".subscriptions WHERE subscription_name = $1",
        [subscription_name]
      )

    last_seen
  end
end
