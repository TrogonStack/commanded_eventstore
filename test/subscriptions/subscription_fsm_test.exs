defmodule EventStore.Subscriptions.SubscriptionFsmTest do
  use EventStore.StorageCase

  alias EventStore.{Storage, UUID}
  alias EventStore.Subscriptions.{SubscriptionFsm, SubscriptionState}

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
      transient: false
    }

    %SubscriptionFsm{state: :subscribed, data: data}
  end

  defp read_subscription(context) do
    %{conn: conn, schema: schema} = context
    %{stream_uuid: stream_uuid, subscription_name: subscription_name} = context

    Storage.Subscription.subscription(conn, stream_uuid, subscription_name, schema: schema)
  end
end
