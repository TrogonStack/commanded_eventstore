defmodule EventStore.Storage.SubscriptionLockRefusedTest do
  use EventStore.StorageCase

  alias EventStore.Storage.Lock

  @database "eventstore_test_xact_lock_denied"
  @role "eventstore_xact_lock_denied"

  setup %{conn: conn, postgrex_config: postgrex_config} do
    drop = fn ->
      Postgrex.query!(conn, ~s|DROP DATABASE IF EXISTS "#{@database}" WITH (FORCE)|, [])
      Postgrex.query!(conn, ~s|DROP ROLE IF EXISTS "#{@role}"|, [])
    end

    drop.()
    on_exit(fn -> drop.() end)

    Postgrex.query!(conn, ~s|CREATE ROLE "#{@role}" LOGIN PASSWORD 'secret'|, [])
    Postgrex.query!(conn, ~s|CREATE DATABASE "#{@database}" TEMPLATE template0|, [])

    {:ok, admin} = Postgrex.start_link(Keyword.put(postgrex_config, :database, @database))

    Postgrex.query!(
      admin,
      "CREATE TABLE public.subscriptions (subscription_id bigint PRIMARY KEY, " <>
        "lock_generation bigint NOT NULL DEFAULT 0, last_seen bigint)",
      []
    )

    Postgrex.query!(admin, "INSERT INTO public.subscriptions (subscription_id) VALUES (1)", [])
    Postgrex.query!(admin, ~s|GRANT SELECT, UPDATE ON public.subscriptions TO "#{@role}"|, [])

    Postgrex.query!(
      admin,
      "REVOKE EXECUTE ON FUNCTION pg_catalog.pg_try_advisory_xact_lock(integer, integer) FROM PUBLIC",
      []
    )

    GenServer.stop(admin)

    role_config =
      Keyword.merge(postgrex_config, database: @database, username: @role, password: "secret")

    [role_conn: start_supervised!({Postgrex, role_config}, id: :role_conn)]
  end

  test "a claim returns the error when only the advisory lock statement is refused", context do
    assert {:error, %Postgrex.Error{postgres: %{code: :insufficient_privilege} = postgres}} =
             Lock.claim_subscription_lock(context.role_conn, 1, schema: "public")

    assert postgres.message =~ "pg_try_advisory_xact_lock"
  end
end
