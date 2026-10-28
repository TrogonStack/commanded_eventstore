defmodule EventStore.TcpProxyTest do
  use ExUnit.Case, async: false

  alias EventStore.{Config, TcpProxy}

  @clients 64

  test "accepts a burst of connections without resetting any of them" do
    config = Config.parsed(TestEventStore, :eventstore)
    proxy = start_supervised!({TcpProxy, {Keyword.fetch!(config, :hostname), config[:port]}})
    port = TcpProxy.port(proxy)

    owner = self()

    sockets =
      1..@clients
      |> Enum.map(fn _ ->
        Task.async(fn ->
          {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
          :ok = :gen_tcp.controlling_process(socket, owner)
          socket
        end)
      end)
      |> Task.await_many()

    Process.sleep(200)

    reset = Enum.count(sockets, &match?({:error, _}, :inet.peername(&1)))

    Enum.each(sockets, &:gen_tcp.close/1)

    assert reset == 0
  end
end
