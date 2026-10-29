defmodule EventStore.Notifications.ListenerTest do
  use EventStore.StorageCase

  @moduletag :capture_log

  alias EventStore.{EventFactory, PubSub, Wait}
  alias EventStore.Notifications.Listener

  @listener TestEventStore.EventStore.Notifications.Listener
  @listen_to TestEventStore.Postgrex.Notifications

  describe "listen_to process down" do
    test "keeps delivering events after the listen_to process dies" do
      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(TestEventStore, stream_uuid)

      listener = whereis(@listener)

      kill(@listen_to)
      wait_until_listening(listener)

      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, events}, 5_000
      assert length(events) == 3
    end

    test "stops with the reason reported for the listen_to process" do
      listener = whereis(@listener)
      ref = Process.monitor(listener)

      kill(@listen_to)

      assert_receive {:DOWN, ^ref, :process, ^listener, reason}, 5_000
      assert reason == :killed
    end

    test "ignores a down message for an unrelated process" do
      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(TestEventStore, stream_uuid)

      listener = whereis(@listener)

      send(listener, {:DOWN, make_ref(), :process, spawn(fn -> :ok end), :normal})

      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, events}, 5_000
      assert length(events) == 3
      assert Process.alive?(listener)
    end
  end

  describe "start" do
    test "catches subscriptions up when the connection it listens on is already listening" do
      event_store = __MODULE__.CaughtUpStore
      registry = Module.concat([event_store, EventStore.Subscriptions.Registry])

      start_supervised!({Registry, keys: :unique, name: registry})
      {:ok, _owner} = Registry.register(registry, "subscription", nil)

      assert {:ok, _ref} = Postgrex.Notifications.listen(@listen_to, "listener_test_probe")

      start_supervised!(
        {Listener,
         event_store: event_store, listen_to: @listen_to, query_timeout: 5_000, schema: "public"}
      )

      assert_receive {:"$gen_cast", :catch_up}
    end
  end

  describe "connecting" do
    test "runs a one argument :configure function with the connection options" do
      assert [host: "configured"] =
               Listener.connecting([host: "original"], make_ref(), fn opts ->
                 Keyword.put(opts, :host, "configured")
               end)
    end

    test "applies a :configure function given as module, function and arguments" do
      assert [extra: 1, host: "original"] =
               Listener.connecting([host: "original"], make_ref(), {Keyword, :put, [:extra, 1]})
    end

    test "keeps the connection options when there is no :configure" do
      assert [host: "original"] = Listener.connecting([host: "original"], make_ref(), nil)
    end
  end

  defp whereis(name) do
    pid = Process.whereis(name)
    assert is_pid(pid)
    pid
  end

  defp kill(name) do
    pid = whereis(name)
    Process.exit(pid, :kill)
  end

  # The supervisor restarts the notifications connection and the listener. Wait
  # for a replacement listener, and for a connection that is actually online:
  # `listen/3` answers `{:eventually, ref}` while it is still reconnecting.
  defp wait_until_listening(previous_listener) do
    Wait.until(10_000, fn ->
      listener = Process.whereis(@listener)

      assert is_pid(listener)
      assert listener != previous_listener
      assert {:ok, ref} = Postgrex.Notifications.listen(@listen_to, "listener_test_probe")
      assert is_reference(ref)
    end)
  end

  defp append_events(stream_uuid, count) do
    TestEventStore.append_to_stream(stream_uuid, :any_version, EventFactory.create_events(count))
  end
end
