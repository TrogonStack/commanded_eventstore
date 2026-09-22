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
        # Shaped after `:proc_lib.stop/3`, which is what `GenServer.stop/3` runs, except that only
        # the subscription can decide whether a subscriber is still connected to it, so asking has
        # to be a call of our own.
        ref = Process.monitor(subscription)

        case stop(subscription) do
          :ok ->
            # Answering is not being gone: the reply is sent before `terminate/2`, where a
            # subscription can still checkpoint. Waiting keeps a stale `last_seen` from landing on
            # whatever row exists by the time it is written. Unbounded, as `GenServer.stop/3` is.
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
  defp stop(subscription) do
    Subscription.stop(subscription)
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
