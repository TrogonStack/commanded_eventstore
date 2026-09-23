defmodule EventStore.Subscriptions.SlowLeaver do
  @moduledoc false

  # Answers a delete and only then takes its time going away, which is the gap a caller that reads
  # being answered as being gone would fall into: the row is deleted and the registered name is
  # still held.

  use GenServer, restart: :temporary

  def start_link(opts) do
    {start_opts, opts} = Keyword.split(opts, [:name])

    GenServer.start_link(__MODULE__, opts, start_opts)
  end

  @impl GenServer
  def init(opts) do
    {:ok,
     %{
       answer_after: Keyword.get(opts, :answer_after, 0),
       leaving_for: Keyword.fetch!(opts, :leaving_for)
     }}
  end

  @impl GenServer
  def handle_call(:delete, from, state) do
    Process.send_after(self(), {:answer, from}, state.answer_after)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:answer, from}, state) do
    GenServer.reply(from, :ok)

    Process.send_after(self(), :leave, state.leaving_for)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info(:leave, state), do: {:stop, :shutdown, state}
end
