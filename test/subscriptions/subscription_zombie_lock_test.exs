defmodule EventStore.Subscriptions.SubscriptionZombieLockTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, TcpProxy, UUID, Wait}
  alias EventStore.Subscriptions.Subscription

  @store_a :zombie_store_a
  @store_b :zombie_store_b

  setup %{config: config, schema: schema} do
    proxy =
      start_supervised!(
        {TcpProxy, {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
      )

    start_supervised!(
      {TestEventStore,
       name: @store_a,
       schema: schema,
       port: TcpProxy.port(proxy),
       session_mode_pool: [timeout: 60_000]},
      id: @store_a
    )

    start_supervised!({TestEventStore, name: @store_b, schema: schema}, id: @store_b)

    [proxy: proxy, subscription_name: UUID.uuid4()]
  end

  test "a node that dies dirty cannot move the progress of the owner that replaced it",
       %{
         conn: conn,
         postgrex_config: postgrex_config,
         schema: schema,
         proxy: proxy,
         subscription_name: subscription_name
       } do
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
    assert 1 == lock_generation(conn, schema, subscription_name)

    hold_unrelated_advisory_locks(conn, postgrex_config, schema, subscription_name)

    :ok = TcpProxy.freeze(proxy)

    test_pid = self()

    pending_ack =
      Task.async(fn ->
        try do
          Subscription.ack(subscription_a, 7, test_pid)
        catch
          :exit, reason -> {:exit, reason}
        end
      end)

    :timer.sleep(300)

    subscriber_b = start_forwarding_subscriber(:b)

    {:ok, subscription_b} =
      TestEventStore.subscribe_to_all_streams(subscription_name, subscriber_b,
        name: @store_b,
        buffer_size: 10
      )

    refute_receive {:b, :subscribed, _subscription}, 2_500
    assert 1 == lock_generation(conn, schema, subscription_name)

    terminate_lock_holder(conn, schema, subscription_name)

    assert_receive {:b, :subscribed, ^subscription_b}, 5_000
    assert_receive {:b, :events, [%{event_number: 4} | _]}, 5_000

    assert 2 == lock_generation(conn, schema, subscription_name)

    send(subscriber_b, {:ack, subscription_b, 5})

    Wait.until(fn -> assert 5 == last_seen(conn, schema, subscription_name) end)

    :ok = TcpProxy.forward(proxy)

    Task.await(pending_ack, 20_000)

    assert 5 == last_seen(conn, schema, subscription_name)
    assert 2 == lock_generation(conn, schema, subscription_name)

    Wait.until(10_000, fn -> assert :disconnected == subscription_state(subscription_a) end)

    assert 5 == last_seen(conn, schema, subscription_name)

    flush_events()
    refute_receive {:events, _events}, 1_500

    send(subscriber_b, {:ack, subscription_b, 6})

    Wait.until(fn -> assert 6 == last_seen(conn, schema, subscription_name) end)
    assert 2 == lock_generation(conn, schema, subscription_name)
  end

  defp terminate_lock_holder(conn, schema, subscription_name) do
    %Postgrex.Result{rows: [[backend_pid]]} =
      Postgrex.query!(
        conn,
        """
        SELECT l.pid
        FROM pg_locks l
        JOIN pg_database d ON d.oid = l.database
        JOIN "#{schema}".subscriptions s ON s.subscription_id = l.objid::bigint
        WHERE l.locktype = 'advisory'
          AND l.granted
          AND d.datname = current_database()
          AND l.classid = '"#{schema}".subscriptions'::regclass::oid
          AND s.subscription_name = $1
        """,
        [subscription_name]
      )

    %Postgrex.Result{rows: [[true]]} =
      Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])
  end

  # Advisory locks are server-wide, so sessions of other databases and other
  # lock users of this one must not be mistaken for the subscription's owner.
  defp hold_unrelated_advisory_locks(conn, postgrex_config, schema, subscription_name) do
    %Postgrex.Result{rows: [[classid, subscription_id]]} =
      Postgrex.query!(
        conn,
        """
        SELECT '"#{schema}".subscriptions'::regclass::oid::int, subscription_id::int
        FROM "#{schema}".subscriptions
        WHERE subscription_name = $1
        """,
        [subscription_name]
      )

    other_database =
      start_supervised!({Postgrex, Keyword.put(postgrex_config, :database, "postgres")},
        id: :other_database
      )

    same_database = start_supervised!({Postgrex, postgrex_config}, id: :same_database)

    Postgrex.query!(other_database, "SELECT pg_advisory_lock($1, $2)", [classid, subscription_id])
    Postgrex.query!(same_database, "SELECT pg_advisory_lock($1)", [subscription_id])
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

  defp lock_generation(conn, schema, subscription_name),
    do: subscription_column(conn, schema, "lock_generation", subscription_name)

  defp last_seen(conn, schema, subscription_name),
    do: subscription_column(conn, schema, "last_seen", subscription_name)

  defp subscription_column(conn, schema, column, subscription_name) do
    %Postgrex.Result{rows: [[value]]} =
      Postgrex.query!(
        conn,
        "SELECT #{column} FROM \"#{schema}\".subscriptions WHERE subscription_name = $1",
        [subscription_name]
      )

    value
  end
end
