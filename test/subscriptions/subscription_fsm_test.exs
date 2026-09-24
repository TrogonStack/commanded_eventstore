defmodule EventStore.Subscriptions.SubscriptionFsmTest do
  use EventStore.StorageCase

  alias EventStore.{Storage, UUID}
  alias EventStore.Subscriptions.{SubscriptionFsm, SubscriptionState}

  # The states that own a stored position, and those that do not.
  @persisting_states [:request_catch_up, :catching_up, :subscribed, :max_capacity]
  @idle_states [:initial, :disconnected, :unsubscribed]

  setup context do
    stream_uuid = UUID.uuid4()
    subscription_name = UUID.uuid4()

    {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
      Storage.subscribe_to_stream(context.conn, stream_uuid, subscription_name,
        schema: context.schema
      )

    [
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscription_id: subscription_id
    ]
  end

  describe "delete/1" do
    test "deletes the row the subscription created", context do
      fsm = fsm(context)

      assert {:ok, %SubscriptionFsm{}} = SubscriptionFsm.delete(fsm)

      assert {:error, :subscription_not_found} = read_subscription(context)
    end

    test "leaves nothing pending for a later checkpoint to write", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3)

      assert {:ok, %SubscriptionFsm{data: %SubscriptionState{checkpoints_pending: 0}}} =
               SubscriptionFsm.delete(fsm)
    end

    test "keeps what was pending when the row it would be written to is still there", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3, schema: "no_such_schema")

      assert {{:error, %Postgrex.Error{}},
              %SubscriptionFsm{data: %SubscriptionState{checkpoints_pending: 1}}} =
               SubscriptionFsm.delete(fsm)

      assert {:ok, %Storage.Subscription{}} = read_subscription(context)
    end

    test "deletes from every state the subscription can be in", context do
      for state <- @persisting_states ++ @idle_states do
        assert {:ok, %SubscriptionFsm{state: ^state}} =
                 SubscriptionFsm.delete(fsm(context, state: state))

        assert {:error, :subscription_not_found} = read_subscription(context)

        recreate_row(context)
      end
    end

    test "reports success when the row is already gone", context do
      :ok =
        Storage.delete_subscription(context.conn, context.stream_uuid, context.subscription_name,
          schema: context.schema
        )

      # A delete asks for the row to be absent, and it is.
      assert {:ok, %SubscriptionFsm{}} = SubscriptionFsm.delete(fsm(context))
    end

    test "leaves no pending checkpoint behind whatever was pending", context do
      for pending <- [0, 1, 5] do
        assert {:ok, %SubscriptionFsm{data: %SubscriptionState{checkpoints_pending: 0}}} =
                 SubscriptionFsm.delete(fsm(context, checkpoints_pending: pending))

        recreate_row(context)
      end
    end

    test "stops a subscription that goes on acknowledging after its row is deleted", context do
      {:ok, %SubscriptionFsm{} = deleted} = SubscriptionFsm.delete(fsm(context))

      # The row is gone but the subscription is still holding subscribers, so the next checkpoint
      # is the moment it finds out.
      SubscriptionFsm.checkpoint(%SubscriptionFsm{
        deleted
        | data: %SubscriptionState{deleted.data | checkpoints_pending: 1, last_ack: 4}
      })

      assert_received {:checkpoint_failed, :subscription_not_found}
    end

    test "removes the row left under the name of a subscription that keeps no position",
         context do
      # A transient subscription has no row of its own, so what it deletes is whatever the name
      # refers to.
      assert {:ok, %SubscriptionFsm{}} = SubscriptionFsm.delete(fsm(context, transient: true))

      assert {:error, :subscription_not_found} = read_subscription(context)
    end

    test "deletes only the subscription it names", context do
      %{conn: conn, schema: schema} = context

      sibling_name = UUID.uuid4()

      {:ok, %Storage.Subscription{}} =
        Storage.subscribe_to_stream(conn, context.stream_uuid, sibling_name, schema: schema)

      assert {:ok, %SubscriptionFsm{}} = SubscriptionFsm.delete(fsm(context))

      assert {:ok, %Storage.Subscription{}} =
               Storage.Subscription.subscription(conn, context.stream_uuid, sibling_name,
                 schema: schema
               )
    end
  end

  describe "checkpoint/1" do
    test "writes the position of the subscription it names", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3)

      assert %SubscriptionFsm{data: %SubscriptionState{checkpoints_pending: 0}} =
               SubscriptionFsm.checkpoint(fsm)

      assert {:ok, %Storage.Subscription{last_seen: 3}} = read_subscription(context)

      refute_received {:checkpoint_failed, _reason}
    end

    test "writes nothing when no acknowledgement is pending", context do
      fsm = fsm(context, checkpoints_pending: 0, last_ack: 3)

      assert %SubscriptionFsm{} = SubscriptionFsm.checkpoint(fsm)

      assert {:ok, %Storage.Subscription{last_seen: nil}} = read_subscription(context)

      refute_received {:checkpoint_failed, _reason}
    end

    test "asks the subscription to stop once the row it names is gone", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3)

      :ok =
        Storage.delete_subscription(context.conn, context.stream_uuid, context.subscription_name,
          schema: context.schema
        )

      assert %SubscriptionFsm{data: %SubscriptionState{checkpoints_pending: 0}} =
               SubscriptionFsm.checkpoint(fsm)

      assert_received {:checkpoint_failed, :subscription_not_found}
    end

    test "does not write the position onto a subscription that reused the name", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid} = context
      %{subscription_name: subscription_name, subscription_id: subscription_id} = context

      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3)

      :ok = Storage.delete_subscription(conn, stream_uuid, subscription_name, schema: schema)

      {:ok, %Storage.Subscription{subscription_id: reused}} =
        Storage.subscribe_to_stream(conn, stream_uuid, subscription_name, schema: schema)

      assert reused != subscription_id

      SubscriptionFsm.checkpoint(fsm)

      assert {:ok, %Storage.Subscription{subscription_id: ^reused, last_seen: nil}} =
               read_subscription(context)
    end

    test "keeps the subscription running when the write fails for a reason of its own", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3, schema: "no_such_schema")

      assert %SubscriptionFsm{} = SubscriptionFsm.checkpoint(fsm)

      # A write that failed is not a row that is gone, and stopping over one would turn every
      # blip in storage into a subscription that has to be restarted.
      refute_received {:checkpoint_failed, _reason}
    end

    test "writes from every state that owns a position", context do
      for state <- @persisting_states do
        fsm = fsm(context, state: state, checkpoints_pending: 1, last_ack: 3)

        assert %SubscriptionFsm{state: ^state} = SubscriptionFsm.checkpoint(fsm)

        assert {:ok, %Storage.Subscription{last_seen: 3}} = read_subscription(context)

        reset_position(context)
      end
    end

    test "writes from no state that does not", context do
      for state <- @idle_states do
        fsm = fsm(context, state: state, checkpoints_pending: 1, last_ack: 3)

        assert %SubscriptionFsm{state: ^state} = SubscriptionFsm.checkpoint(fsm)

        assert {:ok, %Storage.Subscription{last_seen: nil}} = read_subscription(context)
      end

      refute_received {:checkpoint_failed, _reason}
    end

    test "leaves the state it was given alone", context do
      for state <- @persisting_states ++ @idle_states do
        fsm = fsm(context, state: state, checkpoints_pending: 1, last_ack: 3)

        assert %SubscriptionFsm{state: ^state} = SubscriptionFsm.checkpoint(fsm)
      end
    end

    test "writes nothing for a subscription that keeps no position", context do
      fsm = fsm(context, transient: true, checkpoints_pending: 1, last_ack: 3)

      assert %SubscriptionFsm{data: %SubscriptionState{checkpoints_pending: 0}} =
               SubscriptionFsm.checkpoint(fsm)

      # A transient subscription never created a row, so there is none to write to and none to
      # find missing.
      assert {:ok, %Storage.Subscription{last_seen: nil}} = read_subscription(context)

      refute_received {:checkpoint_failed, _reason}
    end

    test "moves the position forward across successive checkpoints", context do
      for position <- [1, 2, 7] do
        fsm = fsm(context, checkpoints_pending: 1, last_ack: position)

        SubscriptionFsm.checkpoint(fsm)

        assert {:ok, %Storage.Subscription{last_seen: ^position}} = read_subscription(context)
      end

      refute_received {:checkpoint_failed, _reason}
    end

    test "writes the same position twice without reporting the row as missing", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3)

      SubscriptionFsm.checkpoint(fsm)
      SubscriptionFsm.checkpoint(fsm)

      assert {:ok, %Storage.Subscription{last_seen: 3}} = read_subscription(context)

      # An acknowledgement that changes no row is still an acknowledgement the row accepted, and
      # reading it as a missing subscription would stop a healthy subscription.
      refute_received {:checkpoint_failed, _reason}
    end

    test "clears what was pending once the row it names is gone", context do
      fsm = fsm(context, checkpoints_pending: 1, last_ack: 3)

      :ok =
        Storage.delete_subscription(context.conn, context.stream_uuid, context.subscription_name,
          schema: context.schema
        )

      # Keeping it pending would have the stopping subscription try the same write again on its
      # way out.
      assert %SubscriptionFsm{data: %SubscriptionState{checkpoints_pending: 0}} =
               SubscriptionFsm.checkpoint(fsm)

      assert_received {:checkpoint_failed, :subscription_not_found}
    end

    test "writes to its own row when another stream reuses the name", context do
      %{conn: conn, schema: schema, subscription_name: subscription_name} = context

      other_stream_uuid = UUID.uuid4()

      {:ok, %Storage.Subscription{subscription_id: other_id}} =
        Storage.subscribe_to_stream(conn, other_stream_uuid, subscription_name, schema: schema)

      assert other_id != context.subscription_id

      SubscriptionFsm.checkpoint(fsm(context, checkpoints_pending: 1, last_ack: 3))

      assert {:ok, %Storage.Subscription{last_seen: 3}} = read_subscription(context)

      # The name is only unique per stream, so a name on its own never identifies a row.
      assert {:ok, %Storage.Subscription{last_seen: nil}} =
               Storage.Subscription.subscription(conn, other_stream_uuid, subscription_name,
                 schema: schema
               )
    end

    test "writes nothing before the subscription has a row of its own", context do
      fsm = %SubscriptionFsm{
        fsm(context, checkpoints_pending: 1, last_ack: 3)
        | state: :initial
      }

      assert %SubscriptionFsm{} = SubscriptionFsm.checkpoint(fsm)

      assert {:ok, %Storage.Subscription{last_seen: nil}} = read_subscription(context)

      refute_received {:checkpoint_failed, _reason}
    end
  end

  defp fsm(context, overrides \\ []) do
    %{conn: conn, schema: schema} = context
    %{stream_uuid: stream_uuid, subscription_name: subscription_name} = context
    %{subscription_id: subscription_id} = context

    data = %SubscriptionState{
      conn: conn,
      schema: Keyword.get(overrides, :schema, schema),
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscription_id: subscription_id,
      last_ack: Keyword.get(overrides, :last_ack, 0),
      checkpoints_pending: Keyword.get(overrides, :checkpoints_pending, 0),
      query_timeout: 15_000,
      transient: Keyword.get(overrides, :transient, false)
    }

    %SubscriptionFsm{state: Keyword.get(overrides, :state, :subscribed), data: data}
  end

  defp reset_position(context) do
    %{conn: conn, schema: schema, subscription_id: subscription_id} = context

    {:ok, _result} =
      Postgrex.query(
        conn,
        ~s|UPDATE "#{schema}".subscriptions SET last_seen = NULL WHERE subscription_id = $1|,
        [subscription_id]
      )

    :ok
  end

  defp recreate_row(context) do
    %{conn: conn, schema: schema} = context
    %{stream_uuid: stream_uuid, subscription_name: subscription_name} = context

    {:ok, %Storage.Subscription{}} =
      Storage.subscribe_to_stream(conn, stream_uuid, subscription_name, schema: schema)

    :ok
  end

  defp read_subscription(context) do
    %{conn: conn, schema: schema} = context
    %{stream_uuid: stream_uuid, subscription_name: subscription_name} = context

    Storage.Subscription.subscription(conn, stream_uuid, subscription_name, schema: schema)
  end
end
