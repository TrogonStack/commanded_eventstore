defmodule EventStore.Notifications.Listener do
  @moduledoc """
  A GenStage producer which listens for event stream notifications from PostgreSQL using the
  PostgreSQL `LISTEN/NOTIFY` mechanism and emits `EventStore.Notifications.Notification` events
  for subscribed `EventStore.Notifications.Publisher` processes.

  The notification payload contains the first and last event number of the appended events.
  """

  use GenStage

  require Logger

  alias EventStore.Notifications
  alias EventStore.Notifications.{Listener, Notification}
  alias EventStore.Subscriptions

  defstruct [
    :event_store,
    :listen_to,
    :monitor_ref,
    :query_timeout,
    :schema,
    :ref,
    demand: 0,
    queue: :queue.new()
  ]

  def start_link(opts) do
    {start_opts, listener_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    event_store = Keyword.fetch!(listener_opts, :event_store)
    listen_to = Keyword.fetch!(listener_opts, :listen_to)
    query_timeout = Keyword.fetch!(listener_opts, :query_timeout)
    schema = Keyword.fetch!(listener_opts, :schema)

    state = %Listener{
      event_store: event_store,
      listen_to: listen_to,
      query_timeout: query_timeout,
      schema: schema
    }

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  @doc false
  # Run by the notifications connection before each attempt to connect. Notifications sent while
  # it is disconnected are never delivered, so every listener sharing the connection is told to
  # find out when it is listening again.
  def connecting(opts, listen_to, configure) do
    Registry.dispatch(Notifications.Registry, {__MODULE__, listen_to}, fn entries ->
      for {pid, _value} <- entries, do: send(pid, :connecting)
    end)

    case configure do
      {module, fun, args} -> apply(module, fun, [opts | args])
      fun when is_function(fun, 1) -> fun.(opts)
      nil -> opts
    end
  end

  def init(%Listener{} = state) do
    %Listener{listen_to: listen_to} = state

    {:ok, _} = Registry.register(Notifications.Registry, {__MODULE__, listen_to}, nil)

    {:producer, listen_for_events(state)}
  end

  # The connection answers a call only once it has connected and run every `LISTEN`, so a call
  # that is answered with `:ok` shows the connection is listening. Connecting can take longer
  # than any query timeout, so the call waits for as long as the connection is alive.
  def handle_info(:connecting, %Listener{} = state) do
    %Listener{listen_to: listen_to, schema: schema} = state

    listener = self()

    {:ok, _pid} = Task.start(fn -> await_listening(listener, listen_to, schema) end)

    {:noreply, [], state}
  end

  def handle_info(:listening, %Listener{} = state) do
    %Listener{event_store: event_store} = state

    :ok = Subscriptions.Supervisor.catch_up(event_store)

    {:noreply, [], state}
  end

  # Notification received from PostgreSQL's `NOTIFY`
  def handle_info({:notification, _connection_pid, _ref, channel, payload}, %Listener{} = state) do
    Logger.debug(
      "Listener received notification on channel " <>
        inspect(channel) <> " with payload: " <> inspect(payload)
    )

    state = Notification.new(payload) |> enqueue(state)

    dispatch_events([], state)
  end

  def handle_info({:DOWN, ref, :process, _object, reason}, %Listener{monitor_ref: ref} = state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, _ref, :process, _object, _reason}, %Listener{} = state) do
    {:noreply, [], state}
  end

  def handle_demand(incoming_demand, %Listener{} = state) do
    %Listener{demand: pending_demand} = state

    state = %Listener{state | demand: pending_demand + incoming_demand}

    dispatch_events([], state)
  end

  defp await_listening(listener, listen_to, schema) do
    case Postgrex.Notifications.listen(listen_to, schema <> ".events", timeout: :infinity) do
      {:ok, _ref} -> send(listener, :listening)
      {:eventually, _ref} -> :ok
    end
  catch
    :exit, _reason -> :ok
  end

  defp listen_for_events(%Listener{} = state) do
    %Listener{listen_to: listen_to, query_timeout: query_timeout, schema: schema} = state

    channel = schema <> ".events"

    ref =
      case Postgrex.Notifications.listen(listen_to, channel, timeout: query_timeout) do
        {:ok, ref} ->
          send(self(), :listening)
          ref

        {:eventually, ref} ->
          ref
      end

    monitor_ref = Process.monitor(listen_to)

    %Listener{state | ref: ref, monitor_ref: monitor_ref}
  end

  defp dispatch_events(events, %Listener{demand: 0} = state) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(events, %Listener{} = state) do
    %Listener{demand: demand, queue: queue} = state

    case :queue.out(queue) do
      {{:value, event}, queue} ->
        state = %Listener{state | demand: max(demand - 1, 0), queue: queue}
        dispatch_events([event | events], state)

      {:empty, _queue} ->
        {:noreply, Enum.reverse(events), state}
    end
  end

  defp enqueue(%Notification{} = notification, %Listener{} = state) do
    %Listener{queue: queue} = state

    %Listener{state | queue: :queue.in(notification, queue)}
  end
end
