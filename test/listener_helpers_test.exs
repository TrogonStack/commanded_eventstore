defmodule EventStore.ListenerHelpersTest do
  use EventStore.StorageCase

  alias EventStore.ListenerHelpers

  setup %{postgrex_config: postgrex_config} do
    application_name = ListenerHelpers.unique_application_name("listener_helpers")

    listener =
      start_supervised!(
        {Postgrex, Keyword.put(postgrex_config, :parameters, application_name: application_name)}
      )

    [listener: listener, application_name: application_name]
  end

  test "does not report listening while LISTEN sits in an open transaction", context do
    %{conn: conn, listener: listener, application_name: application_name} = context

    Postgrex.query!(listener, "BEGIN", [])
    Postgrex.query!(listener, ~s(LISTEN "listener_helpers_channel"), [])

    assert_raise ExUnit.AssertionError, fn ->
      ListenerHelpers.await_listening(conn, application_name, 200)
    end

    Postgrex.query!(listener, "COMMIT", [])
    Postgrex.query!(listener, ~s(LISTEN "listener_helpers_channel"), [])

    assert ListenerHelpers.await_listening(conn, application_name, 1_000)
  end
end
