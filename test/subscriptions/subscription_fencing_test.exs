defmodule EventStore.Subscriptions.SubscriptionFencingTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, TcpProxy, UUID, Wait}
  alias EventStore.Subscriptions.{FencingToken, Subscription}

  @store_a :fenced_store_a
  @store_b :fenced_store_b

  setup %{config: config, schema: schema} do
    proxy =
      start_supervised!(
        {TcpProxy, {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
      )

    start_supervised!(
      {TestEventStore,
       name: @store_a,
       schema: schema,
       session_mode_pool: [port: TcpProxy.port(proxy), timeout: 60_000, idle_interval: 100]},
      id: @store_a
    )

    start_supervised!({TestEventStore, name: @store_b, schema: schema}, id: @store_b)

    [proxy: proxy, subscription_name: UUID.uuid4()]
  end

  test "a subscription that lost its lock does not overwrite the progress of the new owner",
       %{conn: conn, schema: schema, proxy: proxy, subscription_name: subscription_name} do
    {:ok, subscription_a} = subscribe_a(subscription_name, self())

    fence_a(subscription_a, subscription_name, conn, schema, proxy)

    :ok = TestEventStore.ack(subscription_a, 7)

    assert 5 == last_seen(conn, schema, subscription_name)

    Wait.until(fn -> assert :disconnected == subscription_state(subscription_a) end)

    assert 5 == last_seen(conn, schema, subscription_name)

    refute_receive {:events, _events}, 500
  end

  test "a fenced subscription resumes from the committed position when it takes the lock back",
       %{conn: conn, schema: schema, proxy: proxy, subscription_name: subscription_name} do
    {:ok, subscription_a} = subscribe_a(subscription_name, self())

    fence_a(subscription_a, subscription_name, conn, schema, proxy)

    :ok = TestEventStore.ack(subscription_a, 7)

    Wait.until(fn -> assert :disconnected == subscription_state(subscription_a) end)

    assert 5 == last_seen(conn, schema, subscription_name)

    stop_supervised!(@store_b)
    :ok = TcpProxy.forward(proxy)

    assert_receive {:events, [%{event_number: 6} | _]}, 5_000

    assert %FencingToken{generation: 3} = fencing_token(subscription_a)
    assert 3 == lock_generation(conn, schema, subscription_name)

    :ok = TestEventStore.ack(subscription_a, 7)

    Wait.until(fn -> assert 7 == last_seen(conn, schema, subscription_name) end)
  end

  test "a fenced subscription that keeps retrying does not disturb the new owner",
       %{conn: conn, schema: schema, proxy: proxy, subscription_name: subscription_name} do
    {:ok, subscription_a} = subscribe_a(subscription_name, self())

    {subscription_b, subscriber_b} =
      fence_a(subscription_a, subscription_name, conn, schema, proxy)

    :ok = TestEventStore.ack(subscription_a, 7)

    Wait.until(fn -> assert :disconnected == subscription_state(subscription_a) end)

    :timer.sleep(3_500)

    # While its lock pool is unreachable the subscription may sit inside a
    # bounded lock claim, so only the database is probed here.
    assert 2 == lock_generation(conn, schema, subscription_name)
    assert 5 == last_seen(conn, schema, subscription_name)
    refute_received {:events, _events}

    :ok = TcpProxy.forward(proxy)

    :timer.sleep(3_500)

    Wait.until(10_000, fn -> assert :disconnected == subscription_state(subscription_a) end)
    assert 2 == lock_generation(conn, schema, subscription_name)
    assert 5 == last_seen(conn, schema, subscription_name)
    refute_received {:events, _events}

    send(subscriber_b, {:ack, subscription_b, 6})

    Wait.until(fn -> assert 6 == last_seen(conn, schema, subscription_name) end)

    assert :subscribed == subscription_state(subscription_b)
  end

  test "a subscription fenced while catching up disconnects and the new owner resumes from the committed position",
       %{conn: conn, schema: schema, proxy: proxy, subscription_name: subscription_name} do
    for _ <- 1..4, do: append_to_a(50)

    {:ok, subscription_a} = subscribe_a(subscription_name, self(), buffer_size: 1)

    assert_receive {:subscribed, ^subscription_a}

    for event_number <- 1..3 do
      assert_receive {:events, [%{event_number: ^event_number}]}, 5_000

      :ok = TestEventStore.ack(subscription_a, event_number)
    end

    assert_receive {:events, [%{event_number: 4}]}, 5_000

    Wait.until(fn -> assert 3 == last_seen(conn, schema, subscription_name) end)

    refute :subscribed == subscription_state(subscription_a)

    monitor = Process.monitor(subscription_a)

    {subscription_b, subscriber_b} = take_over(subscription_name, conn, schema, proxy)

    :ok = TestEventStore.ack(subscription_a, 4)

    Wait.until(fn -> assert :disconnected == subscription_state(subscription_a) end)

    assert 5 == last_seen(conn, schema, subscription_name)

    flush_events()

    for event_number <- 6..12 do
      send(subscriber_b, {:ack, subscription_b, event_number})
    end

    assert_receive_b_through(15)

    refute_receive {:DOWN, ^monitor, :process, _pid, _reason}, 500
    refute_receive {:events, _events}, 500

    Wait.until(fn -> assert 12 == last_seen(conn, schema, subscription_name) end)
  end

  defp assert_receive_b_through(last_event_number) do
    assert_receive {:b, :events, events}, 5_000

    unless List.last(events).event_number >= last_event_number do
      assert_receive_b_through(last_event_number)
    end
  end

  defp subscribe_a(subscription_name, subscriber, opts \\ []) do
    TestEventStore.subscribe_to_all_streams(
      subscription_name,
      subscriber,
      Keyword.merge([name: @store_a, buffer_size: 10], opts)
    )
  end

  defp fence_a(subscription_a, subscription_name, conn, schema, proxy) do
    assert_receive {:subscribed, ^subscription_a}

    append_to_a(7)

    assert_receive_through(7)

    :ok = TestEventStore.ack(subscription_a, 3)

    Wait.until(fn -> assert 3 == last_seen(conn, schema, subscription_name) end)

    take_over(subscription_name, conn, schema, proxy)
  end

  defp append_to_a(count) do
    :ok =
      TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(count),
        name: @store_a
      )
  end

  # Takes A's lock session away and has B own the subscription at event 5.
  defp take_over(subscription_name, conn, schema, proxy) do
    end_lock_session(conn, proxy)

    subscriber_b = start_forwarding_subscriber(:b)

    {:ok, subscription_b} =
      TestEventStore.subscribe_to_all_streams(subscription_name, subscriber_b,
        name: @store_b,
        buffer_size: 10
      )

    assert_receive {:b, :subscribed, ^subscription_b}, 5_000
    assert_receive {:b, :events, [%{event_number: 4} | _]}, 5_000

    send(subscriber_b, {:ack, subscription_b, 5})

    Wait.until(fn -> assert 5 == last_seen(conn, schema, subscription_name) end)

    flush_events()

    {subscription_b, subscriber_b}
  end

  defp end_lock_session(conn, proxy) do
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
  end

  defp take_over_all(tagged_names, conn, schema, proxy) do
    end_lock_session(conn, proxy)

    owners =
      for {tag, subscription_name} <- tagged_names do
        subscriber = start_forwarding_subscriber(tag)

        {:ok, subscription} =
          TestEventStore.subscribe_to_all_streams(subscription_name, subscriber,
            name: @store_b,
            buffer_size: 10
          )

        assert_receive {^tag, :subscribed, ^subscription}, 5_000
        assert_receive {^tag, :events, [%{event_number: 4} | _]}, 5_000

        send(subscriber, {:ack, subscription, 5})

        Wait.until(fn -> assert 5 == last_seen(conn, schema, subscription_name) end)

        {tag, subscription}
      end

    flush_tagged_events([:a1, :a2, :b1, :b2])

    owners
  end

  defp flush_tagged_events(tags), do: Enum.each(tags, &flush_tagged/1)

  defp flush_tagged(tag) do
    receive do
      {^tag, :events, _events} -> flush_tagged(tag)
    after
      0 -> :ok
    end
  end

  test "every subscription on a node whose lock session ends is fenced and recovers",
       %{conn: conn, schema: schema, proxy: proxy, subscription_name: subscription_name} do
    other_name = UUID.uuid4()
    subscriber_1 = start_forwarding_subscriber(:a1)
    subscriber_2 = start_forwarding_subscriber(:a2)

    {:ok, subscription_1} = subscribe_a(subscription_name, subscriber_1)
    {:ok, subscription_2} = subscribe_a(other_name, subscriber_2)

    assert_receive {:a1, :subscribed, ^subscription_1}
    assert_receive {:a2, :subscribed, ^subscription_2}

    append_to_a(7)

    for tag <- [:a1, :a2], do: assert_receive_tagged_through(tag, 7)

    send(subscriber_1, {:ack, subscription_1, 3})
    send(subscriber_2, {:ack, subscription_2, 3})

    for name <- [subscription_name, other_name] do
      Wait.until(fn -> assert 3 == last_seen(conn, schema, name) end)
    end

    take_over_all([b1: subscription_name, b2: other_name], conn, schema, proxy)

    send(subscriber_1, {:ack, subscription_1, 7})
    send(subscriber_2, {:ack, subscription_2, 7})

    for subscription <- [subscription_1, subscription_2] do
      Wait.until(fn -> assert :disconnected == subscription_state(subscription) end)
    end

    for name <- [subscription_name, other_name] do
      assert 5 == last_seen(conn, schema, name)
    end

    flush_tagged_events([:a1, :a2, :b1, :b2])

    stop_supervised!(@store_b)
    :ok = TcpProxy.forward(proxy)

    assert_receive {:a1, :events, [%{event_number: 6} | _]}, 5_000
    assert_receive {:a2, :events, [%{event_number: 6} | _]}, 5_000

    for name <- [subscription_name, other_name] do
      assert 3 == lock_generation(conn, schema, name)
    end
  end

  defp assert_receive_tagged_through(tag, last_event_number) do
    assert_receive {^tag, :events, events}

    unless List.last(events).event_number >= last_event_number do
      assert_receive_tagged_through(tag, last_event_number)
    end
  end

  test "every subscriber of a fenced subscription stops receiving events",
       %{conn: conn, schema: schema, proxy: proxy, subscription_name: subscription_name} do
    subscriber_1 = start_forwarding_subscriber(:a1)
    subscriber_2 = start_forwarding_subscriber(:a2)

    {:ok, subscription_a} = subscribe_a(subscription_name, subscriber_1, concurrency_limit: 2)
    {:ok, ^subscription_a} = subscribe_a(subscription_name, subscriber_2, concurrency_limit: 2)

    assert_receive {:a1, :subscribed, ^subscription_a}
    assert_receive {:a2, :subscribed, ^subscription_a}

    append_to_a(7)

    owners = receive_owners(%{a1: subscriber_1, a2: subscriber_2}, 7)

    ack_in_order(owners, subscription_a, 1..3)

    Wait.until(fn -> assert 3 == last_seen(conn, schema, subscription_name) end)

    take_over(subscription_name, conn, schema, proxy)

    ack_in_order(owners, subscription_a, 4..7)

    Wait.until(fn -> assert :disconnected == subscription_state(subscription_a) end)

    assert 5 == last_seen(conn, schema, subscription_name)

    refute_receive {:a1, :events, _events}, 500
    refute_receive {:a2, :events, _events}, 0
  end

  defp ack_in_order(owners, subscription, event_numbers) do
    for event_number <- event_numbers do
      send(Map.fetch!(owners, event_number), {:ack, subscription, event_number})
    end
  end

  defp receive_owners(subscribers, last, acc \\ %{}) do
    receive do
      {tag, :events, events} when tag in [:a1, :a2] ->
        acc =
          Enum.reduce(events, acc, &Map.put(&2, &1.event_number, Map.fetch!(subscribers, tag)))

        if map_size(acc) >= last, do: acc, else: receive_owners(subscribers, last, acc)
    after
      2_000 -> acc
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

  defp start_forwarding_subscriber(tag) do
    reply_to = self()

    spawn_link(fn -> forward(tag, reply_to) end)
  end

  defp forward(tag, reply_to) do
    receive do
      {:subscribed, subscription} -> send(reply_to, {tag, :subscribed, subscription})
      {:events, events} -> send(reply_to, {tag, :events, events})
      {:ack, subscription, event_number} -> TestEventStore.ack(subscription, event_number)
    end

    forward(tag, reply_to)
  end

  defp subscription_state(subscription) do
    %Subscription{subscription: %{state: state}} = :sys.get_state(subscription)

    state
  end

  defp fencing_token(subscription) do
    %Subscription{subscription: %{data: %{fencing_token: fencing_token}}} =
      :sys.get_state(subscription)

    fencing_token
  end

  defp lock_generation(conn, schema, subscription_name) do
    %Postgrex.Result{rows: [[generation]]} =
      Postgrex.query!(
        conn,
        "SELECT lock_generation FROM \"#{schema}\".subscriptions WHERE subscription_name = $1",
        [subscription_name]
      )

    generation
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
