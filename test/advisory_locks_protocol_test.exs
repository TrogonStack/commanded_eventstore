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

    assert {:ok, :ok} = Task.yield(closing, 5_000) || Task.shutdown(closing, :brutal_kill)
  end
end
