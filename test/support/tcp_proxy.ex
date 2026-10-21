defmodule EventStore.TcpProxy do
  @moduledoc false

  use GenServer

  @grace 500

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

  @doc """
  Parks every byte and every close on the connections already piped without
  closing any socket, and holds new connections like `blackhole/1`. Both ends
  keep believing the connection is alive. `forward/1` delivers what was parked
  and keeps relaying briefly before resetting those connections, the way late
  packets still arrive when a partition heals.
  """
  def freeze(proxy), do: GenServer.call(proxy, :freeze)

  def stop(proxy), do: GenServer.stop(proxy)

  @impl GenServer
  def init({host, port}) do
    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, listen_port} = :inet.port(listen)
    proxy = self()

    acceptor =
      spawn_link(fn ->
        Process.put(:proxy, proxy)
        accept(listen, proxy, to_charlist(host), port)
      end)

    {:ok,
     %{
       acceptor: acceptor,
       listen: listen,
       port: listen_port,
       mode: :forward,
       held: [],
       relays: []
     }}
  end

  @impl GenServer
  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:mode, _from, state), do: {:reply, state.mode, state}
  def handle_call(:blackhole, _from, state), do: {:reply, :ok, %{state | mode: :blackhole}}

  def handle_call({:hold, client}, _from, state),
    do: {:reply, :ok, %{state | held: [client | state.held]}}

  def handle_call(:freeze, _from, state), do: {:reply, :ok, %{state | mode: :frozen}}

  def handle_call({:register, relay}, _from, state),
    do: {:reply, :ok, %{state | relays: [relay | state.relays]}}

  def handle_call(:forward, _from, state) do
    Enum.each(state.held, &:gen_tcp.close/1)

    if state.mode == :frozen, do: Enum.each(state.relays, &send(&1, :release))

    {:reply, :ok, %{state | mode: :forward, held: [], relays: []}}
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
          mode when mode in [:blackhole, :frozen] -> hold(client, proxy)
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

    proxy = self_proxy()

    for {from, to} <- [{client, upstream}, {upstream, client}] do
      pid = spawn_link(fn -> receive do: (:go -> relay(from, to, proxy, :infinity)) end)
      :ok = :gen_tcp.controlling_process(from, pid)
      :ok = GenServer.call(proxy, {:register, pid})
      send(pid, :go)
    end
  end

  defp self_proxy, do: Process.get(:proxy)

  defp relay(from, to, proxy, deadline) do
    receive do
      :release -> release(from, to, [])
    after
      0 -> relay_once(from, to, proxy, deadline)
    end
  end

  defp relay_once(from, to, proxy, deadline) do
    if expired?(deadline) do
      close(from, to)
    else
      case :gen_tcp.recv(from, 0, 50) do
        {:error, :timeout} ->
          relay(from, to, proxy, deadline)

        {:ok, data} ->
          if deadline == :infinity and GenServer.call(proxy, :mode) == :frozen do
            park(from, to, [data])
          else
            forward_data(from, to, proxy, deadline, data)
          end

        {:error, _reason} ->
          if deadline == :infinity and GenServer.call(proxy, :mode) == :frozen do
            park(from, to, [])
          else
            close(from, to)
          end
      end
    end
  end

  defp expired?(:infinity), do: false
  defp expired?(deadline), do: System.monotonic_time(:millisecond) > deadline

  defp forward_data(from, to, proxy, deadline, data) do
    case :gen_tcp.send(to, data) do
      :ok -> relay(from, to, proxy, deadline)
      {:error, _reason} -> close(from, to)
    end
  end

  defp park(from, to, parked) do
    receive do
      :release -> release(from, to, parked)
    end
  end

  defp release(from, to, parked) do
    Enum.each(Enum.reverse(parked), &:gen_tcp.send(to, &1))
    relay(from, to, nil, System.monotonic_time(:millisecond) + @grace)
  end

  defp close(from, to) do
    :gen_tcp.close(from)
    :gen_tcp.close(to)
  end
end
