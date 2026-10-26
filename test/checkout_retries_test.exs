defmodule EventStore.CheckoutRetriesTest do
  use ExUnit.Case, async: false

  alias EventStore.{Config, EventFactory, TcpProxy, UUID, Wait}

  @event_store :checkout_retries_store
  @checkout_retries 2
  @pool_size 6

  setup do
    config = Config.parsed(TestEventStore, :eventstore)
    port = Keyword.fetch!(config, :port)

    proxy = start_supervised!({TcpProxy, {Keyword.fetch!(config, :hostname), port}})

    start_supervised!(
      {TestEventStore,
       name: @event_store,
       port: TcpProxy.port(proxy),
       session_mode_pool: [port: port],
       pool_size: @pool_size,
       idle_interval: 60_000,
       checkout_retries: @checkout_retries,
       subscription_retry_interval: 60_000}
    )

    conn = Config.lookup(@event_store, :conn)

    1..@pool_size
    |> Enum.map(fn _ ->
      Task.async(fn -> Postgrex.query!(conn, "SELECT pg_sleep(0.2)", []) end)
    end)
    |> Task.await_many()

    :ok = TcpProxy.sever(proxy)

    [proxy: proxy]
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
end
