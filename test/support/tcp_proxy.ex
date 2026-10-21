defmodule EventStore.TcpProxy do
  @moduledoc false

  use GenServer

  def start_link(upstream), do: GenServer.start_link(__MODULE__, upstream)

  def port(proxy), do: GenServer.call(proxy, :port)

  @doc """
  Keeps the connections already piped and holds every new connection open
  without ever answering it.
  """
  def blackhole(proxy), do: GenServer.call(proxy, :blackhole)

  @doc """
  Pipes new connections again and closes the ones held open by `blackhole/1`
  so their clients fail fast and reconnect.
  """
  def forward(proxy), do: GenServer.call(proxy, :forward)

  def stop(proxy), do: GenServer.stop(proxy)

  @impl GenServer
  def init({host, port}) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, listen_port} = :inet.port(listen)
    proxy = self()
    acceptor = spawn_link(fn -> accept(listen, proxy, to_charlist(host), port) end)

    {:ok, %{acceptor: acceptor, listen: listen, port: listen_port, mode: :forward, held: []}}
  end

  @impl GenServer
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:mode, _from, state), do: {:reply, state.mode, state}
  def handle_call(:blackhole, _from, state), do: {:reply, :ok, %{state | mode: :blackhole}}

  def handle_call({:hold, client}, _from, state),
    do: {:reply, :ok, %{state | held: [client | state.held]}}

  def handle_call(:forward, _from, state) do
    Enum.each(state.held, &:gen_tcp.close/1)

    {:reply, :ok, %{state | mode: :forward, held: []}}
  end

  @impl GenServer
  def terminate(_reason, %{acceptor: acceptor, listen: listen}) do
    Process.unlink(acceptor)
    Process.exit(acceptor, :kill)
    :gen_tcp.close(listen)
  end

  defp accept(listen, proxy, host, port) do
    case :gen_tcp.accept(listen) do
      {:ok, client} ->
        case GenServer.call(proxy, :mode) do
          :forward -> pipe(client, host, port)
          :blackhole -> hold(client, proxy)
        end

        accept(listen, proxy, host, port)

      {:error, _reason} ->
        :ok
    end
  end

  defp hold(client, proxy) do
    :ok = :gen_tcp.controlling_process(client, proxy)
    :ok = GenServer.call(proxy, {:hold, client})
  end

  defp pipe(client, host, port) do
    {:ok, upstream} = :gen_tcp.connect(host, port, [:binary, active: false])

    for {from, to} <- [{client, upstream}, {upstream, client}] do
      pid = spawn_link(fn -> receive do: (:go -> relay(from, to)) end)
      :ok = :gen_tcp.controlling_process(from, pid)
      send(pid, :go)
    end
  end

  defp relay(from, to) do
    with {:ok, data} <- :gen_tcp.recv(from, 0),
         :ok <- :gen_tcp.send(to, data) do
      relay(from, to)
    else
      {:error, _reason} ->
        :gen_tcp.close(from)
        :gen_tcp.close(to)
    end
  end
end
