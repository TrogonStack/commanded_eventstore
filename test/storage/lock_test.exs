defmodule EventStore.Storage.LockTest do
  use EventStore.StorageCase

  alias EventStore.ProcessHelper
  alias EventStore.Storage.{Lock, Session}
  alias EventStore.Wait

  test "acquire exclusive subscription lock", %{postgrex_config: postgrex_config, schema: schema} do
    conn = start_supervised!({Postgrex, postgrex_config})

    assert :ok = Lock.try_acquire_exclusive_lock(conn, random_key(), schema: schema)
  end

  test "acquire and release lock by connection", %{
    postgrex_config: postgrex_config,
    schema: schema
  } do
    {:ok, conn1} = Postgrex.start_link(postgrex_config)
    {:ok, conn2} = Postgrex.start_link(postgrex_config)

    key = random_key()

    # conn1 acquire lock
    assert :ok = Lock.try_acquire_exclusive_lock(conn1, key, schema: schema)

    # conn2 cannot acquire lock
    assert {:error, :lock_already_taken} =
             Lock.try_acquire_exclusive_lock(conn2, key, schema: schema)

    # conn1 can acquire same lock multiple times
    assert :ok = Lock.try_acquire_exclusive_lock(conn1, key, schema: schema)

    # shutdown conn1 process should release its locks
    ProcessHelper.shutdown(conn1)

    # conn2 can now acquire lock
    Wait.until(fn ->
      assert :ok = Lock.try_acquire_exclusive_lock(conn2, key, schema: schema)
    end)

    ProcessHelper.shutdown(conn2)
  end

  describe "on a connection that switches role after connecting" do
    test "acquire exclusive lock", %{postgrex_config: postgrex_config, schema: schema} do
      conn1 = start_switched_role_connection(postgrex_config, :conn1)
      conn2 = start_switched_role_connection(postgrex_config, :conn2)

      key = random_key()

      assert :ok = Lock.try_acquire_exclusive_lock(conn1, key, schema: schema)

      assert {:error, :lock_already_taken} =
               Lock.try_acquire_exclusive_lock(conn2, key, schema: schema)
    end

    test "acquire exclusive session lock", %{postgrex_config: postgrex_config, schema: schema} do
      conn1 = start_switched_role_connection(postgrex_config, :conn1)
      conn2 = start_switched_role_connection(postgrex_config, :conn2)

      assert {:ok, %Session{} = session} =
               Lock.try_acquire_exclusive_session_lock(conn1, random_key(), schema: schema)

      assert {:ok, ^session} = Session.current(conn1, [])
      assert {:ok, ^session} = Session.current(conn1, [])

      assert {:ok, %Session{} = other_session} = Session.current(conn2, [])
      refute other_session.id == session.id
    end
  end

  defp start_switched_role_connection(postgrex_config, id) do
    postgrex_config =
      Keyword.put(
        postgrex_config,
        :after_connect,
        {Postgrex, :query!, ["SET ROLE pg_signal_backend", []]}
      )

    start_supervised!({Postgrex, postgrex_config}, id: id)
  end

  def random_key, do: :rand.uniform(2_147_483_647)
end
