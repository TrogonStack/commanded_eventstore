defmodule EventStore.Storage.SubscriptionFencingPropertyTest do
  use EventStore.StorageCase
  use ExUnitProperties

  alias EventStore.{Storage, UUID, Wait}
  alias EventStore.Storage.Lock

  @sessions [:one, :two, :three]

  defmodule Model do
    defstruct owner: nil, generation: 0, last_seen: nil, tokens: %{}, connections: %{}
  end

  property "progress only moves forward and only for the latest lock owner", context do
    check all(commands <- list_of(command(), max_length: 12), max_runs: 30) do
      subscription_id = create_subscription(context)
      model = start_sessions(%Model{}, context)

      try do
        Enum.reduce(commands, model, fn command, model ->
          model = execute(command, model, subscription_id, context)

          assert {model.generation, model.last_seen} == read(subscription_id, context)

          model
        end)
      after
        delete_subscription(subscription_id, context)
        stop_sessions(model)
      end
    end
  end

  defp command do
    session = member_of(@sessions)

    one_of([
      tuple({constant(:claim), session}),
      tuple({constant(:ack), session, integer(0..50)}),
      tuple({constant(:drop), session}),
      tuple({constant(:take_over), session})
    ])
  end

  defp execute({:take_over, session}, %Model{owner: owner} = model, subscription_id, context)
       when owner not in [nil, session] do
    model = execute({:drop, owner}, model, subscription_id, context)

    execute({:claim, session}, model, subscription_id, context)
  end

  defp execute({:take_over, session}, %Model{} = model, subscription_id, context) do
    execute({:claim, session}, model, subscription_id, context)
  end

  defp execute({:claim, session}, %Model{owner: owner} = model, subscription_id, context)
       when owner not in [nil, session] do
    assert {:error, :lock_already_taken} = claim(model, session, subscription_id, context)

    model
  end

  defp execute({:claim, session}, %Model{} = model, subscription_id, context) do
    assert {:ok, lease} = claim(model, session, subscription_id, context)

    assert lease.last_seen == model.last_seen
    assert lease.token.generation == model.generation + 1

    %Model{
      model
      | owner: session,
        generation: model.generation + 1,
        tokens: Map.put(model.tokens, session, lease.token)
    }
  end

  defp execute({:ack, session, position}, %Model{} = model, _subscription_id, context) do
    case Map.get(model.tokens, session) do
      nil ->
        model

      %{generation: generation} = token when generation == model.generation ->
        assert :ok = ack(model, session, token, position, context)

        %Model{model | last_seen: max(model.last_seen || position, position)}

      token ->
        assert {:error, :fenced} = ack(model, session, token, position, context)

        model
    end
  end

  defp execute({:drop, session}, %Model{} = model, _subscription_id, context) do
    model = end_session(model, session, context)

    %Model{model | owner: if(model.owner == session, do: nil, else: model.owner)}
  end

  defp claim(model, session, subscription_id, %{schema: schema}) do
    Lock.claim_subscription_lock(connection(model, session), subscription_id, schema: schema)
  end

  defp ack(model, session, token, position, %{schema: schema}) do
    Storage.ack_last_seen_event(connection(model, session), token, position, schema: schema)
  end

  defp stop_sessions(%Model{connections: connections}) do
    Enum.each(connections, fn {_session, connection} -> GenServer.stop(connection) end)
  end

  defp connection(%Model{connections: connections}, session), do: Map.fetch!(connections, session)

  defp start_sessions(%Model{} = model, %{postgrex_config: postgrex_config}) do
    connections =
      Map.new(@sessions, fn session ->
        {:ok, connection} = Postgrex.start_link(postgrex_config)

        {session, connection}
      end)

    %Model{model | connections: connections}
  end

  defp end_session(%Model{} = model, session, %{conn: conn}) do
    connection = connection(model, session)

    %Postgrex.Result{rows: [[backend_pid]]} =
      Postgrex.query!(connection, "SELECT pg_backend_pid()", [])

    %Postgrex.Result{rows: [[true]]} =
      Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

    Wait.until(fn ->
      assert %Postgrex.Result{rows: [[0]]} =
               Postgrex.query!(
                 conn,
                 "SELECT count(*)::int FROM pg_stat_activity WHERE pid = $1",
                 [backend_pid]
               )
    end)

    Wait.until(fn -> assert {:ok, _result} = Postgrex.query(connection, "SELECT 1", []) end)

    model
  end

  defp create_subscription(%{conn: conn, schema: schema}) do
    {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
      Storage.subscribe_to_stream(conn, "$all", UUID.uuid4(), schema: schema)

    subscription_id
  end

  defp delete_subscription(subscription_id, %{conn: conn}) do
    Postgrex.query!(conn, "DELETE FROM public.subscriptions WHERE subscription_id = $1", [
      subscription_id
    ])
  end

  defp read(subscription_id, %{conn: conn}) do
    %Postgrex.Result{rows: [[generation, last_seen]]} =
      Postgrex.query!(
        conn,
        "SELECT lock_generation, last_seen FROM public.subscriptions WHERE subscription_id = $1",
        [subscription_id]
      )

    {generation, last_seen}
  end
end
