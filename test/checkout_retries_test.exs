defmodule EventStore.CheckoutRetriesTest do
  use ExUnit.Case, async: false

  alias EventStore.{Config, EventFactory, ListenerHelpers, Serializer, TcpProxy, UUID, Wait}
  alias EventStore.Subscriptions.{FencingToken, SubscriptionFsm, SubscriptionState}

  @event_store :checkout_retries_store
  @checkout_retries 2
  @pool_size 12

  setup context do
    config = Config.parsed(TestEventStore, :eventstore)
    port = Keyword.fetch!(config, :port)

    application_name = ListenerHelpers.unique_application_name("checkout_retries")
    direct = start_supervised!({Postgrex, Config.default_postgrex_opts(config)})

    proxy = start_supervised!({TcpProxy, {Keyword.fetch!(config, :hostname), port}})

    start_supervised!(
      {TestEventStore,
       name: @event_store,
       port: TcpProxy.port(proxy),
       session_mode_pool: [port: port, parameters: [application_name: application_name]],
       pool_size: @pool_size,
       idle_interval: 60_000,
       checkout_retries: Map.get(context, :checkout_retries, @checkout_retries),
       subscription_retry_interval: 60_000}
    )

    conn = Config.lookup(@event_store, :conn)

    ListenerHelpers.await_listening(direct, application_name)

    1..@pool_size
    |> Enum.map(fn _ ->
      Task.async(fn -> Postgrex.query!(conn, "SELECT pg_sleep(0.2)", []) end)
    end)
    |> Task.await_many()

    :ok = TcpProxy.sever(proxy)

    [proxy: proxy, config: config]
  end

  test "appending to a stream retries checkouts as many times as configured", %{proxy: proxy} do
    events = EventFactory.create_events(1)

    assert {:error, %DBConnection.ConnectionError{}} =
             TestEventStore.append_to_stream(UUID.uuid4(), :any_version, events,
               name: @event_store
             )

    assert TcpProxy.severed(proxy) == @checkout_retries + 1
  end

  test "subscribing from the current stream version retries checkouts as many times as configured",
       %{proxy: proxy} do
    assert {:error, %DBConnection.ConnectionError{}} =
             TestEventStore.subscribe_to_stream(UUID.uuid4(), UUID.uuid4(), self(),
               name: @event_store,
               start_from: :current
             )

    assert TcpProxy.severed(proxy) == @checkout_retries + 1
  end

  test "a subscription retries checkouts as many times as configured", %{proxy: proxy} do
    {:ok, _subscription} =
      TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(), name: @event_store)

    Wait.until(5_000, fn -> assert TcpProxy.severed(proxy) > 0 end)
    Process.sleep(500)

    assert TcpProxy.severed(proxy) == @checkout_retries + 1
  end

  @tag checkout_retries: 0
  test "reading what a notification announced does not retry checkouts when none are configured",
       %{proxy: proxy} do
    start_supervised!(
      {TestEventStore, name: :checkout_retries_direct_store}
      |> Supervisor.child_spec(id: :checkout_retries_direct_store)
    )

    :ok =
      TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(1),
        name: :checkout_retries_direct_store
      )

    Wait.until(5_000, fn -> assert TcpProxy.severed(proxy) > 0 end)
    Process.sleep(500)

    assert TcpProxy.severed(proxy) == 2
  end

  describe "a subscription" do
    test "deleting its row retries checkouts as many times as configured", context do
      attempt(fn -> context |> fsm() |> SubscriptionFsm.delete() end)

      assert TcpProxy.severed(context.proxy) == @checkout_retries + 1
    end

    test "writing its checkpoint retries checkouts as many times as configured", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3)

      attempt(fn -> SubscriptionFsm.checkpoint(fsm) end)

      assert TcpProxy.severed(context.proxy) == @checkout_retries + 1
    end

    test "reading the events it has not seen retries checkouts as many times as configured",
         context do
      fsm = fsm(context, state: :request_catch_up)

      attempt(fn -> SubscriptionFsm.catch_up(fsm) end)

      assert TcpProxy.severed(context.proxy) == @checkout_retries + 1
    end
  end

  defp attempt(fun) do
    fun.()
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end

  defp fsm(%{config: config}, overrides \\ []) do
    conn = Config.lookup(@event_store, :conn)

    data = %SubscriptionState{
      conn: conn,
      schema: "public",
      stream_uuid: UUID.uuid4(),
      subscription_name: UUID.uuid4(),
      subscription_id: 1,
      fencing_token: FencingToken.new(1, 1),
      last_ack: Keyword.get(overrides, :last_ack, 0),
      checkpoints_pending: Keyword.get(overrides, :checkpoints_pending, 0),
      retry_interval: 60_000,
      query_timeout: 15_000,
      checkout_opts: Config.checkout_opts(checkout_retries: @checkout_retries),
      event_store: @event_store,
      serializer: Serializer.serializer(TestEventStore, config),
      max_size: 1_000
    }

    %SubscriptionFsm{state: Keyword.get(overrides, :state, :subscribed), data: data}
  end
end
