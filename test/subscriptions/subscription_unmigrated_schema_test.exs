defmodule EventStore.Subscriptions.SubscriptionUnmigratedSchemaTest do
  use EventStore.StorageCase

  import ExUnit.CaptureLog

  alias EventStore.{AdvisoryLocks, EventFactory, UUID, Wait}
  alias EventStore.Subscriptions.Subscription

  @advisory_locks TestEventStore.EventStore.AdvisoryLocks
  @advisory_locks_conn TestEventStore.EventStore.AdvisoryLocks.Postgrex

  test "a database without the lock generation keeps a new node retrying instead of crashing",
       %{conn: conn} do
    on_exit(fn -> add_lock_generation(conn) end)

    Postgrex.query!(conn, "ALTER TABLE subscriptions DROP COLUMN lock_generation", [])

    advisory_locks = Process.whereis(@advisory_locks)
    advisory_locks_conn = Process.whereis(@advisory_locks_conn)

    {:ok, subscription} =
      TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(), buffer_size: 10)

    ref = Process.monitor(subscription)

    refute_receive {:subscribed, _subscription}, 1_500
    refute_receive {:DOWN, ^ref, :process, _pid, _reason}

    assert :initial == subscription_state(subscription)
    assert advisory_locks == Process.whereis(@advisory_locks)
    assert advisory_locks_conn == Process.whereis(@advisory_locks_conn)

    add_lock_generation(conn)

    assert_receive {:subscribed, ^subscription}, 3_000

    :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(1))

    assert_receive {:events, [%{event_number: 1}]}

    Wait.until(fn -> assert :subscribed == subscription_state(subscription) end)
  end

  test "a database without the lock generation tells the operator to migrate, once",
       %{conn: conn} do
    on_exit(fn -> add_lock_generation(conn) end)

    Postgrex.query!(conn, "ALTER TABLE subscriptions DROP COLUMN lock_generation", [])

    log =
      capture_log(fn ->
        {:ok, _subscription} =
          TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(), buffer_size: 10)

        claim_repeatedly(@advisory_locks, :undefined_column)

        refute_receive {:subscribed, _subscription}
      end)

    assert [_reported] = Regex.scan(~r/\[error\].*TestEventStore/, log)
    assert log =~ "v1.5.0"
    assert log =~ "mix event_store.migrate"
  end

  test "a session pool role that cannot update subscriptions is named as the cause, once",
       %{conn: conn, schema: schema} do
    role = "es_lock_role_" <> String.replace(UUID.uuid4(), "-", "")
    password = UUID.uuid4()

    on_exit(fn -> drop_role(conn, schema, role) end)

    Postgrex.query!(conn, ~s(CREATE ROLE "#{role}" LOGIN PASSWORD '#{password}'), [])
    Postgrex.query!(conn, ~s(GRANT USAGE ON SCHEMA "#{schema}" TO "#{role}"), [])
    Postgrex.query!(conn, ~s(GRANT SELECT ON "#{schema}".subscriptions TO "#{role}"), [])

    start_supervised!(
      {TestEventStore,
       name: :limited_lock_role_store,
       schema: schema,
       session_mode_pool: [username: role, password: password]},
      id: :limited_lock_role_store
    )

    log =
      capture_log(fn ->
        {:ok, _subscription} =
          TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(),
            name: :limited_lock_role_store,
            buffer_size: 10
          )

        claim_repeatedly(
          Module.concat([:limited_lock_role_store, AdvisoryLocks]),
          :insufficient_privilege
        )

        refute_receive {:subscribed, _subscription}
      end)

    assert [_reported] = Regex.scan(~r/\[error\].*limited_lock_role_store/, log)
    assert log =~ "UPDATE"
    assert log =~ ~s("#{schema}".subscriptions)
  end

  # Every claim fails the same way, so each one is a chance to report it again.
  defp claim_repeatedly(advisory_locks, code) do
    for _attempt <- 1..3 do
      assert {:error, %Postgrex.Error{postgres: %{code: ^code}}} =
               AdvisoryLocks.try_subscription_lock(advisory_locks, 1, 5_000)
    end
  end

  defp drop_role(conn, schema, role) do
    Postgrex.query!(conn, ~s(REVOKE ALL ON ALL TABLES IN SCHEMA "#{schema}" FROM "#{role}"), [])
    Postgrex.query!(conn, ~s(REVOKE ALL ON SCHEMA "#{schema}" FROM "#{role}"), [])
    Postgrex.query!(conn, ~s(DROP ROLE IF EXISTS "#{role}"), [])
  end

  defp add_lock_generation(conn) do
    Postgrex.query!(
      conn,
      "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS lock_generation bigint NOT NULL DEFAULT 0",
      []
    )
  end

  defp subscription_state(subscription) do
    %Subscription{subscription: %{state: state}} = :sys.get_state(subscription)

    state
  end
end
