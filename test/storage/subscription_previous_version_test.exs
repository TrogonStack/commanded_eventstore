defmodule EventStore.Storage.SubscriptionPreviousVersionTest do
  use EventStore.StorageCase

  alias EventStore.ProcessHelper
  alias EventStore.Storage.Lock
  alias EventStore.Subscriptions.FencingToken

  @insert_subscription """
  INSERT INTO "public".subscriptions
    (
      stream_uuid,
      subscription_name,
      last_seen
    )
  VALUES ($1, $2, $3)
  RETURNING subscription_id, stream_uuid, subscription_name, last_seen, created_at;
  """

  @try_advisory_lock """
  SELECT pg_try_advisory_lock(
    '"public".subscriptions'::regclass::oid::int,
    (CASE WHEN $1 > 2147483647 THEN mod($1, 2147483647) ELSE $1 END)::int
  );
  """

  @subscription_ack """
  UPDATE "public".subscriptions
  SET last_seen = $2
  WHERE subscription_id = $1;
  """

  @query_subscription """
  SELECT
    subscription_id,
    stream_uuid,
    subscription_name,
    last_seen,
    created_at
  FROM "public".subscriptions
  WHERE stream_uuid = $1 AND subscription_name = $2;
  """

  setup %{postgrex_config: postgrex_config} do
    {:ok, previous_node} = Postgrex.start_link(postgrex_config)

    on_exit(fn -> ProcessHelper.shutdown(previous_node) end)

    [previous_node: previous_node]
  end

  test "a node on the previous version keeps working against the migrated schema", context do
    %{conn: conn, previous_node: previous_node, schema: schema} = context

    %Postgrex.Result{rows: [[subscription_id | _columns]]} =
      Postgrex.query!(previous_node, @insert_subscription, ["$all", "previous_version", nil])

    assert 0 == lock_generation(conn, subscription_id)

    assert %Postgrex.Result{rows: [[true]]} =
             Postgrex.query!(previous_node, @try_advisory_lock, [subscription_id])

    Postgrex.query!(previous_node, @subscription_ack, [subscription_id, 7])

    assert 0 == lock_generation(conn, subscription_id)

    assert %Postgrex.Result{
             rows: [[^subscription_id, "$all", "previous_version", 7, _created_at]]
           } =
             Postgrex.query!(previous_node, @query_subscription, ["$all", "previous_version"])

    assert {:error, :lock_already_taken} =
             Lock.claim_subscription_lock(conn, subscription_id, schema: schema)

    :ok = Lock.unlock(previous_node, subscription_id, schema: schema)

    assert {:ok, lease} = Lock.claim_subscription_lock(conn, subscription_id, schema: schema)
    assert lease.token == FencingToken.new(subscription_id, 1)
    assert lease.last_seen == 7
  end

  defp lock_generation(conn, subscription_id) do
    %Postgrex.Result{rows: [[generation]]} =
      Postgrex.query!(
        conn,
        "SELECT lock_generation FROM public.subscriptions WHERE subscription_id = $1",
        [subscription_id]
      )

    generation
  end
end
