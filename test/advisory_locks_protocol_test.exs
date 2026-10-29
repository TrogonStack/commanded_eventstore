defmodule EventStore.AdvisoryLocks.ProtocolTest do
  use EventStore.StorageCase

  alias EventStore.{Config, TcpProxy}
  alias EventStore.AdvisoryLocks.Protocol

  test "closing a connection that cannot cancel its query gives up after a second even with an infinite timeout" do
    config = Config.parsed(TestEventStore, :eventstore)

    proxy =
      start_supervised!(
        {TcpProxy, {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
      )

    opts =
      config
      |> Config.advisory_locks_postgrex_opts()
      |> Keyword.merge(port: TcpProxy.port(proxy), timeout: :infinity)
      |> Postgrex.Utils.default_opts()

    assert {:ok, state} = Protocol.connect(opts)

    :ok = TcpProxy.freeze(proxy)

    closing =
      Task.async(fn ->
        Protocol.disconnect(%DBConnection.ConnectionError{message: "closed"}, state)
      end)

    assert {:ok, :ok} = Task.yield(closing, 1_800) || Task.shutdown(closing, :brutal_kill)
  end

  test "a query that fails with the connection closed announces the lost session" do
    config = Config.parsed(TestEventStore, :eventstore)

    proxy =
      start_supervised!(
        {TcpProxy, {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
      )

    opts =
      config
      |> Config.advisory_locks_postgrex_opts()
      |> Keyword.merge(port: TcpProxy.port(proxy))
      |> Postgrex.Utils.default_opts()

    assert {:ok, state} = Protocol.connect(opts)

    :ok = TcpProxy.sever(proxy)

    query = %Postgrex.Query{name: "", statement: "SELECT 1"}

    assert {:disconnect, %DBConnection.ConnectionError{}, _state} =
             Protocol.handle_prepare(query, [], state)

    assert_received {EventStore.AdvisoryLocks, :query_disconnected}
  end
end
