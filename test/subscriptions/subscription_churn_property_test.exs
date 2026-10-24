defmodule EventStore.Subscriptions.SubscriptionChurnPropertyTest do
  use EventStore.StorageCase
  use ExUnitProperties

  alias EventStore.{EventFactory, UUID, Wait}
  alias EventStore.Subscriptions.Subscription

  @moduletag :slow
  @moduletag timeout: 600_000

  @nodes [1, 2, 3]

  defmodule Subscriber do
    @moduledoc false

    use GenServer

    def start(), do: GenServer.start(__MODULE__, nil)

    def seen(subscriber), do: GenServer.call(subscriber, :seen)
    def subscription(subscriber), do: GenServer.call(subscriber, :subscription)

    def ack(subscriber, event_number),
      do: GenServer.call(subscriber, {:ack, event_number}, 20_000)

    @impl GenServer
    def init(_), do: {:ok, %{subscription: nil, seen: MapSet.new()}}

    @impl GenServer
    def handle_info({:subscribed, subscription}, state),
      do: {:noreply, %{state | subscription: subscription}}

    def handle_info({:events, events}, state) do
      seen = Enum.reduce(events, state.seen, &MapSet.put(&2, &1.event_number))

      {:noreply, %{state | seen: seen}}
    end

    @impl GenServer
    def handle_call(:seen, _from, state),
      do: {:reply, state.seen |> MapSet.to_list() |> Enum.sort(:desc), state}

    def handle_call(:subscription, _from, state), do: {:reply, state.subscription, state}

    def handle_call({:ack, _event_number}, _from, %{subscription: nil} = state),
      do: {:reply, {:error, :not_subscribed}, state}

    def handle_call({:ack, event_number}, _from, state),
      do: {:reply, Subscription.ack(state.subscription, event_number, self()), state}
  end

  defmodule Model do
    @moduledoc false

    defstruct subscription_name: nil,
              subscribers: %{},
              generation: 0,
              last_seen: 0,
              legit_max: 0
  end

  property "progress only follows the owner of the current generation across node churn",
           %{conn: conn, schema: schema} do
    check all(
            commands <- resize(list_of(command(), max_length: 10), 10),
            max_runs: 10,
            max_shrinking_steps: 20
          ) do
      model = %Model{subscription_name: UUID.uuid4()}

      try do
        model =
          Enum.reduce(commands, model, fn command, model ->
            model
            |> apply_command(command, conn, schema)
            |> check_invariants(model, conn, schema)
          end)

        finish(model, conn, schema)
      after
        for node <- @nodes, do: stop_node(%Model{}, node)
        :timer.sleep(100)
      end
    end
  end

  defp command do
    node = member_of(@nodes)

    one_of([
      tuple({constant(:start), node}),
      tuple({constant(:stop), node}),
      tuple({constant(:kill_lock_session), node}),
      tuple({constant(:append), integer(1..5)}),
      tuple({constant(:ack), node, integer(1..5)}),
      constant(:settle)
    ])
  end

  defp store_name(node), do: :"churn_node_#{node}"

  defp apply_command(%Model{} = model, {:start, node}, _conn, schema) do
    if Map.has_key?(model.subscribers, node), do: model, else: start_node(model, node, schema)
  end

  defp apply_command(%Model{} = model, {:stop, node}, _conn, _schema), do: stop_node(model, node)

  defp apply_command(%Model{} = model, {:kill_lock_session, node}, conn, _schema) do
    with true <- Map.has_key?(model.subscribers, node),
         {:ok, backend_pid} <- lock_session_backend_pid(node) do
      Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])
    end

    model
  end

  defp apply_command(%Model{} = model, {:append, count}, _conn, _schema) do
    append(count)

    model
  end

  defp apply_command(%Model{} = model, {:ack, node, k}, conn, schema) do
    case Map.fetch(model.subscribers, node) do
      {:ok, subscriber} ->
        case Enum.at(Subscriber.seen(subscriber), k - 1) do
          nil -> model
          event_number -> ack(model, node, event_number, conn, schema)
        end

      :error ->
        model
    end
  end

  defp apply_command(%Model{} = model, :settle, conn, schema), do: settle(model, conn, schema)

  # A node whose lock session was already killed has no lock connection to
  # ask until its pool comes back, so a repeated kill has nothing to do.
  defp lock_session_backend_pid(node) do
    lock_pool = Module.concat([store_name(node), EventStore.AdvisoryLocks, Postgrex])

    case Postgrex.query(lock_pool, "SELECT pg_backend_pid()", []) do
      {:ok, %Postgrex.Result{rows: [[backend_pid]]}} -> {:ok, backend_pid}
      {:error, _reason} -> :error
    end
  catch
    :exit, _reason -> :error
  end

  defp start_node(%Model{} = model, node, schema) do
    start_supervised!({TestEventStore, name: store_name(node), schema: schema},
      id: store_name(node)
    )

    {:ok, subscriber} = Subscriber.start()

    {:ok, _subscription} =
      TestEventStore.subscribe_to_all_streams(model.subscription_name, subscriber,
        name: store_name(node),
        buffer_size: 10
      )

    %Model{model | subscribers: Map.put(model.subscribers, node, subscriber)}
  end

  defp stop_node(%Model{} = model, node) do
    _ = stop_supervised(store_name(node))

    case Map.pop(model.subscribers, node) do
      {nil, _subscribers} ->
        model

      {subscriber, subscribers} ->
        Process.exit(subscriber, :kill)
        %Model{model | subscribers: subscribers}
    end
  end

  defp append(count) do
    :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(count))
  end

  # An ack counts as made by the owner when the acking node holds the
  # generation the database holds around the time it acks. A takeover that
  # completes while the ack is in flight is read as the owner's ack rather
  # than failing the run.
  defp ack(%Model{} = model, node, event_number, conn, schema) do
    subscriber = Map.fetch!(model.subscribers, node)

    legit_before? = holds_current_generation?(subscriber, model, conn, schema)

    _ = Subscriber.ack(subscriber, event_number)

    if legit_before? or holds_current_generation?(subscriber, model, conn, schema),
      do: %Model{model | legit_max: max(model.legit_max, event_number)},
      else: model
  end

  defp holds_current_generation?(subscriber, %Model{} = model, conn, schema) do
    case subscription_row(conn, schema, model.subscription_name) do
      {generation, _last_seen} -> token_generation(subscriber) == generation
      nil -> false
    end
  end

  defp token_generation(subscriber) do
    with subscription when is_pid(subscription) <- Subscriber.subscription(subscriber),
         %Subscription{subscription: %{data: %{fencing_token: token}}}
         when token != nil <- :sys.get_state(subscription, 5_000) do
      token.generation
    else
      _ -> nil
    end
  catch
    :exit, _ -> nil
  end

  defp subscription_state(subscriber) do
    with subscription when is_pid(subscription) <- Subscriber.subscription(subscriber),
         %Subscription{subscription: %{state: state}} <- :sys.get_state(subscription, 5_000) do
      state
    else
      _ -> nil
    end
  catch
    :exit, _ -> nil
  end

  defp settle(%Model{subscribers: subscribers} = model, _conn, _schema)
       when map_size(subscribers) == 0,
       do: model

  defp settle(%Model{} = model, conn, schema) do
    max_event = max_event_number(conn)

    Wait.until(15_000, fn ->
      for {node, subscriber} <- model.subscribers, do: ack_all_seen(model, node, subscriber)

      owners =
        for {_node, subscriber} <- model.subscribers,
            subscription_state(subscriber) == :subscribed,
            do: subscriber

      assert [_owner] = owners

      {_generation, last_seen} = subscription_row(conn, schema, model.subscription_name)
      assert last_seen >= max_event
    end)

    {generation, _last_seen} = subscription_row(conn, schema, model.subscription_name)

    owner_seen =
      for {_node, subscriber} <- model.subscribers,
          token_generation(subscriber) == generation,
          event_number <- Subscriber.seen(subscriber),
          do: event_number

    %Model{model | legit_max: Enum.max([model.legit_max | owner_seen])}
  end

  defp ack_all_seen(_model, _node, subscriber) do
    for event_number <- subscriber |> Subscriber.seen() |> Enum.reverse() do
      Subscriber.ack(subscriber, event_number)
    end
  end

  defp check_invariants(%Model{} = model, previous, conn, schema) do
    case subscription_row(conn, schema, model.subscription_name) do
      nil ->
        model

      {generation, last_seen} ->
        assert generation >= previous.generation,
               "lock_generation went from #{previous.generation} to #{generation}"

        assert last_seen >= previous.last_seen,
               "last_seen went from #{previous.last_seen} to #{last_seen}"

        assert last_seen <= model.legit_max,
               "last_seen #{last_seen} exceeds the highest event acked by a holder of the " <>
                 "current generation (#{model.legit_max})"

        %Model{model | generation: generation, last_seen: last_seen}
    end
  end

  defp finish(%Model{} = model, conn, schema) do
    model = Enum.reduce(@nodes, model, &apply_command(&2, {:start, &1}, conn, schema))
    model = settle(model, conn, schema)

    append(3)
    final = max_event_number(conn)

    Wait.until(15_000, fn ->
      for {node, subscriber} <- model.subscribers do
        ack_all_seen(model, node, subscriber)
      end

      {_generation, last_seen} = subscription_row(conn, schema, model.subscription_name)
      assert last_seen == final
    end)

    delivered =
      for {node, subscriber} <- model.subscribers,
          Enum.any?(Subscriber.seen(subscriber), &(&1 > final - 3)),
          do: node

    assert [_one_node] = delivered
  end

  defp max_event_number(conn) do
    %Postgrex.Result{rows: [[max]]} =
      Postgrex.query!(conn, "SELECT stream_version FROM streams WHERE stream_uuid = '$all'", [])

    max
  end

  defp subscription_row(conn, schema, subscription_name) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT lock_generation, COALESCE(last_seen, 0) FROM \"#{schema}\".subscriptions WHERE subscription_name = $1",
        [subscription_name]
      )

    case rows do
      [[generation, last_seen]] -> {generation, last_seen}
      [] -> nil
    end
  end
end
