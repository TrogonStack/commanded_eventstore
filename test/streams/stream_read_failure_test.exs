defmodule EventStore.Streams.StreamReadFailureTest do
  use EventStore.StorageCase

  alias EventStore.EventFactory
  alias EventStore.UUID

  @name :impatient_reads
  @conn Module.concat([@name, Postgrex])

  setup do
    start_supervised!(
      {TestEventStore, name: @name, pool_size: 1, queue_target: 1, queue_interval: 1}
    )

    stream_uuid = UUID.uuid4()

    :ok = TestEventStore.subscribe(stream_uuid, name: @name)
    :ok = TestEventStore.subscribe("$all", name: @name)

    :ok =
      TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3), name: @name)

    assert_receive {:events, _events}
    assert_receive {:events, _events}

    [stream_uuid: stream_uuid]
  end

  test "should raise a read error when a forward batch read fails", %{stream_uuid: stream_uuid} do
    stream = TestEventStore.stream_forward(stream_uuid, 0, name: @name)

    hold_connection(fn ->
      error =
        assert_raise EventStore.StreamReadError, fn -> Enum.to_list(stream) end

      assert error.stream_uuid == stream_uuid
      assert error.reason =~ "connection not available"
    end)
  end

  test "should raise a read error when a backward batch read fails", %{stream_uuid: stream_uuid} do
    stream = TestEventStore.stream_backward(stream_uuid, -1, name: @name)

    hold_connection(fn ->
      error =
        assert_raise EventStore.StreamReadError, fn -> Enum.to_list(stream) end

      assert error.stream_uuid == stream_uuid
      assert error.reason =~ "connection not available"
    end)
  end

  defp hold_connection(fun) do
    test_process = self()

    holder =
      spawn_link(fn ->
        Postgrex.transaction(@conn, fn _conn ->
          send(test_process, :connection_held)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :connection_held

    try do
      fun.()
    after
      send(holder, :release)
    end
  end
end
