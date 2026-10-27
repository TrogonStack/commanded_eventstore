defmodule EventStore.Subscriptions.SubscriptionListenGapTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, ListenerHelpers, TcpProxy, UUID, Wait}

  @store :listen_gap_store
  @notifications_conn Module.concat([@store, Postgrex, Notifications])

  setup %{config: config, conn: conn, schema: schema} do
    proxy =
      start_supervised!(
        {TcpProxy, {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
      )

    application_name = ListenerHelpers.unique_application_name("listen_gap")

    start_supervised!(
      {TestEventStore,
       name: @store,
       schema: schema,
       configure: {__MODULE__, :configure, [self()]},
       session_mode_pool: [
         port: TcpProxy.port(proxy),
         idle_interval: 100,
         parameters: [application_name: application_name]
       ]},
      id: @store
    )

    ListenerHelpers.await_listening(conn, application_name)

    [proxy: proxy, application_name: application_name]
  end

  test "a caught up subscription hears events appended while the notification connection was down",
       %{conn: conn, proxy: proxy, application_name: application_name} do
    {:ok, subscription} =
      TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(), name: @store, buffer_size: 10)

    assert_receive {:subscribed, ^subscription}

    append(1)

    assert_receive {:events, [%{event_number: 1}]}, 5_000
    :ok = TestEventStore.ack(subscription, 1)

    :ok = TcpProxy.blackhole(proxy)
    terminate_listener(conn, application_name)

    append(1)

    :ok = TcpProxy.forward(proxy)

    assert_receive {:events, [%{event_number: 2}]}, 5_000
  end

  test "the notification connection runs the configured :configure before connecting" do
    notifications = Process.whereis(@notifications_conn)

    assert_receive {:configure, ^notifications}
  end

  def configure(opts, test) do
    send(test, {:configure, self()})

    opts
  end

  defp append(count) do
    :ok =
      TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(count),
        name: @store
      )
  end

  defp terminate_listener(conn, application_name) do
    %Postgrex.Result{rows: [[true]]} =
      Postgrex.query!(
        conn,
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1 AND query LIKE '%LISTEN %'",
        [application_name]
      )

    Wait.until(fn ->
      assert %Postgrex.Result{rows: [[false]]} =
               Postgrex.query!(
                 conn,
                 "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND query LIKE '%LISTEN %')",
                 [application_name]
               )
    end)
  end
end
