defmodule EventStore.Storage.Lock do
  @moduledoc false

  alias EventStore.Sql.Statements
  alias EventStore.Storage.Lock.Lease
  alias EventStore.Storage.Session
  alias EventStore.Subscriptions.FencingToken

  @doc """
  Use two 32-bit key values for advisory locks where the first key acts as the
  namespace.

  The namespace key is derived from the unique `oid` value for the `subscriptions`
  table. The `oid` is unique within a database and differs for identically named
  tables defined in different schemas and on repeat table definitions.

  This change aims to prevent lock collision with application level
  advisory lock usage and other libraries using Postgres advisory locks. Now
  there is a 1 in 2,147,483,647 chance of colliding with other locks.
  """
  def try_acquire_exclusive_lock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.try_advisory_lock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> :ok
      {:ok, %Postgrex.Result{rows: [[false]]}} -> {:error, :lock_already_taken}
      {:error, _error} = reply -> reply
    end
  end

  @doc """
  Like `try_acquire_exclusive_lock/3`, but also returns the session holding the
  lock, which releases it when it ends.
  """
  def try_acquire_exclusive_session_lock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.try_advisory_session_lock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true, backend_pid, session_id]]}} ->
        {:ok, Session.new(backend_pid, session_id)}

      {:ok, %Postgrex.Result{rows: [[false, _backend_pid, _session_id]]}} ->
        {:error, :lock_already_taken}

      {:error, _error} = reply ->
        reply
    end
  end

  @doc """
  Takes the advisory lock of a subscription and bumps its `lock_generation` in
  the same statement, returning the lease that fences the subscription's
  progress writes to this lock owner.
  """
  def claim_subscription_lock(conn, subscription_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.claim_subscription_lock(schema)

    case Postgrex.query(conn, query, [subscription_id], opts) do
      {:ok, %Postgrex.Result{rows: [[false, _backend_pid, _session_id, _generation, _last_seen]]}} ->
        {:error, :lock_already_taken}

      {:ok, %Postgrex.Result{rows: [[true, _backend_pid, _session_id, nil, nil]]}} ->
        with :ok <- unlock(conn, subscription_id, [schema: schema] ++ opts) do
          {:error, :subscription_not_found}
        end

      {:ok, %Postgrex.Result{rows: [[true, backend_pid, session_id, generation, last_seen]]}} ->
        token = FencingToken.new(subscription_id, generation)

        {:ok, Lease.new(token, last_seen, Session.new(backend_pid, session_id))}

      {:error, _error} = reply ->
        reply
    end
  end

  def unlock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.advisory_unlock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> :ok
      {:ok, %Postgrex.Result{rows: [[false]]}} -> :ok
      {:error, _error} = reply -> reply
    end
  end
end
