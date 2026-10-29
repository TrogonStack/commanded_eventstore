defmodule EventStore.Storage.Lock do
  @moduledoc false

  alias EventStore.Sql.Statements
  alias EventStore.Storage.Lock.Lease
  alias EventStore.Storage.Session
  alias EventStore.Subscriptions.FencingToken

  @default_timeout 15_000

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
  the same transaction, returning the lease that fences the subscription's
  progress writes to this lock owner.
  """
  def claim_subscription_lock(conn, subscription_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    # The probe fails on a schema or lock role that cannot write the row without
    # writing it, so a node that loses every claim still learns of the problem.
    # The transaction scoped lock tells a lost claim apart before the row is
    # written, and it ends with the transaction, so a failed claim never has to
    # give back a session lock that may be a hold its owner already had.
    with_bounded_lock_wait(conn, opts, fn conn ->
      with :ok <- probe_lock_generation(conn, schema, opts),
           :ok <- try_acquire_exclusive_transaction_lock(conn, subscription_id, schema, opts),
           {:ok, %FencingToken{} = token, last_seen} <-
             bump_lock_generation(conn, subscription_id, schema, opts),
           {:ok, %Session{} = session} <-
             try_acquire_exclusive_session_lock(conn, subscription_id, [schema: schema] ++ opts) do
        {:ok, Lease.new(token, last_seen, session)}
      end
    end)
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

  defp probe_lock_generation(conn, schema, opts) do
    case Postgrex.query(conn, Statements.probe_lock_generation(schema), [], opts) do
      {:ok, %Postgrex.Result{}} -> :ok
      {:error, _error} = reply -> reply
    end
  end

  defp try_acquire_exclusive_transaction_lock(conn, subscription_id, schema, opts) do
    query = Statements.try_advisory_xact_lock(schema)

    case Postgrex.query(conn, query, [subscription_id], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> :ok
      {:ok, %Postgrex.Result{rows: [[false]]}} -> {:error, :lock_already_taken}
      {:error, _error} = reply -> reply
    end
  end

  defp bump_lock_generation(conn, subscription_id, schema, opts) do
    query = Statements.bump_lock_generation(schema)

    case Postgrex.query(conn, query, [subscription_id], opts) do
      {:ok, %Postgrex.Result{rows: [[generation, last_seen]]}} ->
        {:ok, FencingToken.new(subscription_id, generation), last_seen}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:error, :subscription_not_found}

      {:error, _error} = reply ->
        reply
    end
  end

  # A query that outlives its timeout drops the connection along with every lock
  # the session holds, so waiting on a lock held on the subscriptions table gives
  # up well before that.
  defp with_bounded_lock_wait(conn, opts, fun) do
    Postgrex.transaction(
      conn,
      fn conn ->
        with {:ok, _result} <-
               Postgrex.query(conn, Statements.bound_lock_wait(), [lock_wait(opts)], opts),
             {:ok, result} <- fun.(conn) do
          result
        else
          {:error, error} -> Postgrex.rollback(conn, error)
        end
      end,
      opts
    )
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] -> {:error, error}
  end

  defp lock_wait(opts) do
    case Keyword.get(opts, :timeout, @default_timeout) do
      :infinity -> 0
      timeout -> div(timeout, 2)
    end
  end
end
