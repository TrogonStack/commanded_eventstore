defmodule EventStore.Subscriptions.SubscriptionUnmigratedSchemaTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, UUID, Wait}
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
