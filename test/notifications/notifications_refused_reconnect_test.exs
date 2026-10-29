defmodule EventStore.Notifications.NotificationsRefusedReconnectTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, ListenerHelpers, UUID, Wait}

  @store :refused_reconnect_store
  @notifications_conn Module.concat([@store, Postgrex, Notifications])

  setup %{config: config, conn: conn, schema: schema} do
    application_name = ListenerHelpers.unique_application_name("refused_reconnect")
    port = Keyword.fetch!(config, :port)
    target = start_supervised!({Agent, fn -> port end})

    start_supervised!(
      {TestEventStore,
       name: @store,
       schema: schema,
       timeout: 1_000,
       configure: {__MODULE__, :configure, [target]},
       session_mode_pool: [
         idle_interval: 100,
         parameters: [application_name: application_name]
       ]},
      id: @store
    )

    ListenerHelpers.await_listening(conn, application_name)

    [application_name: application_name, port: port, target: target]
  end

  test "the notification connection survives reconnect attempts that are refused",
       %{conn: conn, application_name: application_name, port: port, target: target} do
    {:ok, subscription} =
      TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(), name: @store, buffer_size: 10)

    assert_receive {:subscribed, ^subscription}

    notifications = Process.whereis(@notifications_conn)
    ref = Process.monitor(notifications)

    :ok = Agent.update(target, fn _port -> unused_port() end)
    terminate_listener(conn, application_name)

    refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 2_500

    :ok = Agent.update(target, fn _port -> port end)

    Wait.until(10_000, fn ->
      :ok = append(1)
      assert_receive {:events, [_event]}, 200
    end)
  end

  def configure(opts, target) do
    Keyword.put(opts, :port, Agent.get(target, & &1))
  end

  defp unused_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp append(count) do
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
  end
end
