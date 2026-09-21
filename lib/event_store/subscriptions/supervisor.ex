defmodule EventStore.Subscriptions.Supervisor do
  @moduledoc false

  # Supervise zero, one or more subscriptions to an event stream.

  use DynamicSupervisor

  alias EventStore.Subscriptions
  alias EventStore.Subscriptions.Subscription

  def start_link(opts) do
    DynamicSupervisor.start_link(__MODULE__, [], opts)
  end

  def start_subscription(opts) do
    event_store = Keyword.fetch!(opts, :event_store)
    stream_uuid = Keyword.fetch!(opts, :stream_uuid)
    subscription_name = Keyword.fetch!(opts, :subscription_name)

    supervisor = Module.concat(event_store, __MODULE__)

    via_name = {:via, Registry, registry_name(event_store, stream_uuid, subscription_name)}
    opts = Keyword.put(opts, :name, via_name)

    DynamicSupervisor.start_child(supervisor, {Subscription, opts})
  end

  def unsubscribe_from_stream(event_store, stream_uuid, subscription_name) do
    name = registry_name(event_store, stream_uuid, subscription_name)

    case Registry.whereis_name(name) do
      :undefined ->
        :ok

      subscription ->
        Subscription.unsubscribe(subscription)
    end
  end

  def stop_subscription(event_store, stream_uuid, subscription_name) do
    name = registry_name(event_store, stream_uuid, subscription_name)

    case Registry.whereis_name(name) do
      :undefined ->
        :ok

      subscription ->
        ref = Process.monitor(subscription)

        # Letting the subscription itself decide keeps a subscriber that connects concurrently from
        # being torn down, and waiting for it to go down keeps the checkpoint written while it
        # terminates from racing the caller deleting the subscription it belongs to.
        case stop_unless_subscribed(subscription) do
          :ok ->
            receive do
              {:DOWN, ^ref, :process, ^subscription, _reason} -> :ok
            end

          {:error, _error} = error ->
            Process.demonitor(ref, [:flush])

            error
        end
    end
  end

  # A subscription that goes down while being asked to stop leaves nothing to stop. Its last
  # subscriber has most likely just unsubscribed, which answers before the subscription it stops
  # has terminated. A timeout is not that, and has to reach the caller.
  defp stop_unless_subscribed(subscription) do
    Subscription.stop_unless_subscribed(subscription)
  catch
    :exit, {reason, {GenServer, :call, _args}} when reason != :timeout -> :ok
  end

  @impl DynamicSupervisor
  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  defp registry_name(event_store, stream_uuid, subscription_name) do
    registry = Module.concat([event_store, Subscriptions.Registry])

    {registry, {stream_uuid, subscription_name}}
  end
end
