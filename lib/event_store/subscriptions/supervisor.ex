defmodule EventStore.Subscriptions.Supervisor do
  @moduledoc false

  # Supervise zero, one or more subscriptions to an event stream.

  use DynamicSupervisor

  alias EventStore.Storage
  alias EventStore.Subscriptions
  alias EventStore.Subscriptions.Subscription

  # Mirrors the storage `:timeout` a subscription gives its own queries, for the internal callers
  # that have no configured event store to read one from.
  @default_timeout 15_000

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

  def delete_subscription(event_store, conn, stream_uuid, subscription_name, opts \\ []) do
    name = registry_name(event_store, stream_uuid, subscription_name)

    case Registry.whereis_name(name) do
      :undefined ->
        Storage.delete_subscription(conn, stream_uuid, subscription_name, opts)

      subscription ->
        # Shaped after `:proc_lib.stop/3`, which is what `GenServer.stop/3` runs, except that only
        # the subscription can decide whether a subscriber is still connected to it, so asking has
        # to be a call of our own.
        ref = Process.monitor(subscription)
        asked_at = System.monotonic_time(:millisecond)

        case delete(subscription, opts) do
          :gone ->
            Process.demonitor(ref, [:flush])

            Storage.delete_subscription(conn, stream_uuid, subscription_name, opts)

          {:error, _error} = error ->
            Process.demonitor(ref, [:flush])

            error

          reply ->
            # Answering is not being gone, and a caller that subscribes again under the same name
            # the moment it is answered races a process that still holds it. One deadline covers
            # being answered and being gone.
            receive do
              {:DOWN, ^ref, :process, ^subscription, _reason} -> reply
            after
              remaining(opts, asked_at) ->
                Process.demonitor(ref, [:flush])

                exit({:timeout, {__MODULE__, :delete_subscription, [subscription]}})
            end
        end
    end
  end

  # A subscription that goes down while being asked to delete itself has not deleted its row, and
  # has given up the name that was keeping anything else from claiming it, which leaves the row to
  # delete from here. A timeout is not that: it says the subscription may still be writing, so
  # deleting the row from here would delete it out from under that write.
  defp delete(subscription, opts) do
    Subscription.delete(subscription, opts)
  catch
    :exit, {reason, {GenServer, :call, _args}} when reason != :timeout -> :gone
  end

  defp remaining(opts, asked_at) do
    case Keyword.get(opts, :timeout, @default_timeout) do
      :infinity -> :infinity
      timeout -> max(timeout - (System.monotonic_time(:millisecond) - asked_at), 0)
    end
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
