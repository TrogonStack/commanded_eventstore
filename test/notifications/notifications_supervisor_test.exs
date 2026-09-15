defmodule EventStore.Notifications.NotificationsSupervisorTest do
  use EventStore.StorageCase

  alias EventStore.{EventFactory, Notifications, PubSub, Wait}

  describe "notifications supervisor" do
    setup do
      config =
        TestEventStore.config()
        |> Keyword.put(:subscription_hibernate_after, 0)

      conn = start_supervised!({Postgrex, config})

      start_supervised!({Notifications.Supervisor, {ES, Keyword.put(config, :conn, conn)}})

      for child_spec <- PubSub.child_spec(ES), do: start_supervised!(child_spec)

      :ok
    end

    test "hibernate processes after inactivity" do
      listener_pid = Module.concat([ES, EventStore.Notifications.Listener]) |> Process.whereis()

      publisher_name_pid =
        Module.concat([ES, EventStore.Notifications.Publisher]) |> Process.whereis()

      # Listener processes should be hibernated after inactivity
      Wait.until(fn ->
        assert_hibernated(listener_pid)
        assert_hibernated(publisher_name_pid)
      end)

      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(ES, stream_uuid)

      # Appending events to the event store should resume listener processes
      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, _events}

      # Listener processes should be hibernated again after inactivity
      Wait.until(fn ->
        assert_hibernated(listener_pid)
        assert_hibernated(publisher_name_pid)
      end)
    end
  end

  # OTP 28 reports the `gen_server` hibernation loop where earlier releases
  # reported `:erlang.hibernate/3`.
  @hibernated_functions [{:erlang, :hibernate, 3}, {:gen_server, :loop_hibernate, 4}]

  defp assert_hibernated(pid) do
    assert {:current_function, current_function} = Process.info(pid, :current_function)
    assert current_function in @hibernated_functions
  end

  defp append_events(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version)

    TestEventStore.append_to_stream(stream_uuid, expected_version, events)
  end
end
