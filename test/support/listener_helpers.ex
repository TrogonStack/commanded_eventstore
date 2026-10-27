defmodule EventStore.ListenerHelpers do
  import ExUnit.Assertions

  alias EventStore.Wait

  def unique_application_name(prefix), do: "#{prefix}_#{System.unique_integer([:positive])}"

  # A notification sent before the listener has run LISTEN is lost, and nothing else would deliver
  # events appended after a subscription has caught up.
  def await_listening(conn, application_name) do
    Wait.until(10_000, fn ->
      assert %Postgrex.Result{rows: [[true]]} =
               Postgrex.query!(
                 conn,
                 "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND query LIKE 'LISTEN %')",
                 [application_name]
               )
    end)
  end
end
