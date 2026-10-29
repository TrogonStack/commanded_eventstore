# Running on a cluster of nodes

EventStore supports running on multiple nodes as either a [distributed Erlang](http://erlang.org/doc/reference_manual/distributed.html) cluster or as multiple single instance nodes.

## Event publication

PostgreSQL's `LISTEN` / `NOTIFY` is used to pub/sub event notifications. A listener database connection process is started on each node. It connects to the database to listen for events and publishes them to interested subscription processes running on the node. The approach is the same regardless of whether distributed Erlang is used or not.

## Subscriptions

PostgreSQL's [advisory locks](https://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS) are used to limit each uniquely named subscription to run at most once. This prevents multiple instances of a subscription from running on different nodes. Advisory locks are faster than table locks, are stored in memory to avoid table bloat, and are automatically cleaned up by the server at the end of the session.

The lock decides which node may record the subscription's progress, not which node may run its handler. A node only learns that its lock session ended after the fact, so for a short time two nodes can both process the same events. Each time the lock is acquired the subscription row records a new lock generation, and every checkpoint is written against the generation its owner holds. A node whose lock was taken over has its checkpoints rejected, stops sending events to its subscribers, and tries to subscribe again later. Its subscribers still handle the events already sent to them, and their acks for those events are ignored. The node that took over resumes from the last checkpoint committed before the hand-off, and a checkpoint can never move the subscription's position backwards.

Subscribers must therefore be idempotent, as they are for any at-least-once delivery. This protection takes effect once every node runs a version that records lock generations; until then, nodes on an older version behave as before.

When the lock connection stalls, each attempt a subscription makes to take its lock waits for up to the event store's `timeout` (15 seconds by default), and the subscription makes another attempt every `:subscription_retry_interval` for as long as the connection stays stalled. A subscriber that calls `ack/2` on that subscription during an attempt can exit with a `:timeout` after the default 5 second call timeout, and that exposure comes back on every retry, so acks can keep timing out for far longer than a single `timeout`. The subscription's position is unaffected: a subscription waiting for its lock ignores those acks anyway, and it resumes from the last committed checkpoint once it holds the lock again. Subscribers that ack from a process that must not crash should catch that exit.

### How long a node takes to learn its lock session ended

Subscriptions hold their locks on a dedicated connection. When the database stops answering on that connection, how soon the node stops its subscriptions depends on what that connection was doing:

- While a lock query is running, the node learns of the loss once the query reaches its `:timeout` (15,000ms by default), plus up to two seconds spent trying to cancel it. With `timeout: :infinity` the query never gives up and `ping_timeout` does not apply to it, so the node is not told until the socket itself errors, and until then its subscriptions keep running on a session that has ended. Keep `:timeout` finite on any node that runs subscriptions.
- While the connection is idle, the node learns of the loss when the connection next pings the database: within one to two times `idle_interval` (1,000ms by default), plus however long the ping waits for a reply. That wait is `ping_timeout`, which defaults to `:timeout`. With `timeout: :infinity` the ping waits forever, so set `ping_timeout` in `session_mode_pool` to bound it, for example `session_mode_pool: [ping_timeout: 15_000]`.

PostgreSQL only releases the locks of a session once it notices the client is gone. When a node's host dies or the network drops without closing the connection, the server only notices through TCP keepalives, which with the Linux defaults takes over two hours. Set `tcp_keepalives_idle`, `tcp_keepalives_interval` and `tcp_keepalives_count` together on the server to shorten it, for example to 10, 5 and 3 for a release within about 25 seconds; `tcp_keepalives_idle` alone still leaves the probes to the default interval and count, and `client_connection_check_interval` does not help, since it only notices a connection the operating system already knows is closed. Until then no other node can take the lock, so a slow release delays the hand-off but never lets two nodes record progress at the same time.

### Keepalives when PgBouncer is in front of the lock connection

When `session_mode_url` points at PgBouncer in session mode, as suggested in [Using with PgBouncer](Getting Started.md#using-with-pgbouncer), the server's keepalives no longer protect you. The client of the lock session, as PostgreSQL sees it, is PgBouncer, which is still running after a node's host dies, so the server finds nothing wrong and the session keeps its advisory locks. They are released only when PgBouncer notices that its own client is gone and resets the server connection. By default PgBouncer leaves that to the operating system's keepalive settings, which on Linux default to 7,200 seconds before the first probe and then 9 probes 75 seconds apart. That is roughly 2 hours and 11 minutes before PgBouncer can notice, and the lock is released only after it does, so the actual wait depends on PgBouncer and the network as well. Until then no other node can take over the subscriptions.

Configure the keepalives on PgBouncer as well, in the `[pgbouncer]` section of `pgbouncer.ini`, with the same values as the server advice above:

```ini
[pgbouncer]
tcp_keepalive = 1
tcp_keepidle = 10
tcp_keepintvl = 5
tcp_keepcnt = 3
```

`tcp_keepalive` is on by default and uses the operating system's values; `tcp_keepidle`, `tcp_keepintvl` and `tcp_keepcnt` are unset by default and override them, so set them together. In a test that cut a node off from PgBouncer, these values released the lock after about 27 seconds, the same as a connection straight to PostgreSQL, while with the defaults it was still held after 120 seconds. Keep the server's own keepalives as well, since they cover PgBouncer's host dying.

Do not use PgBouncer's `client_idle_timeout` for this. The lock connection is idle between queries by design, so the timeout would close healthy lock sessions and make subscriptions lose their locks.

Keepalives set on the node's side, such as `socket_options: [keepalive: true]` in `session_mode_pool`, do not help here. Those probes are sent by the node's host, which is the machine that died, so nothing is left to send them.

## Automatic cluster formation

You can use [libcluster](https://github.com/bitwalker/libcluster) to automatically form clusters of Erlang nodes, with either static or dynamic node membership.

You will need to include `libcluster` as an additional dependency in `mix.exs`:

```elixir
defp deps do
  [{:libcluster, "~> 2.2"}]
end
```

Then configure your preferred cluster topology in the environment config (e.g. `config/config.exs`). An example is shown below using the standard Erlang `epmd` daemon strategy:

```elixir
config :libcluster,
  topologies: [
    example: [
      strategy: Cluster.Strategy.Epmd,
      config: [hosts: [:"node1@127.0.0.1", :"node2@127.0.0.1", :"node3@127.0.0.1"]],
    ]
  ]
```

Please refer to the [`libcluster` documentation](https://hexdocs.pm/libcluster/) for more detail.

### Starting a cluster

  1. Run an [Erlang Port Mapper Daemon](http://erlang.org/doc/man/epmd.html) (epmd):

      ```console
      $ epmd -d
      ```

  2. Start an `iex` console per node:

      ```console
      $ MIX_ENV=distributed iex --name node1@127.0.0.1 -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node2@127.0.0.1 -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node3@127.0.0.1 -S mix
      ```

The cluster will be automatically formed as soon as the nodes start.

## Static cluster topology and formation

Instead of using `libcluster` you can configure the `:kernel` application to wait for cluster formation before starting your application during node start up. This approach is useful when you have a static cluster topology that can be defined in config.

The `sync_nodes_optional` configuration specifies which nodes to attempt to connect to within the `sync_nodes_timeout` window, defined in milliseconds, before continuing with startup. There is also a `sync_nodes_mandatory` setting which can be used to enforce all nodes are connected within the timeout window or else the node terminates.

Each node requires its own individual configuration, listing the other nodes in the cluster:

```elixir
# node1 config
config :kernel,
  sync_nodes_optional: [:"node2@192.168.1.1", :"node3@192.168.1.2"],
  sync_nodes_timeout: 30_000
```

The `sync_nodes_timeout` can be configured as `:infinity` to wait indefinitely for all nodes to
connect. All involved nodes must have the same value for `sync_nodes_timeout`.

The above approach will *only work* for Elixir releases. You will need to use [Erlang's `sys.config`](http://erlang.org/doc/man/config.html) file for development purposes.

The Erlang equivalent of the `:kernerl` mix config, as above, is:

```erlang
% node1.sys.config
[{kernel,
  [
    {sync_nodes_optional, ['node2@127.0.0.1', 'node3@127.0.0.1']},
    {sync_nodes_timeout, 30000}
  ]}
].
```

### Starting a cluster

  1. Run an [Erlang Port Mapper Daemon](http://erlang.org/doc/man/epmd.html) (epmd):

      ```console
      $ epmd -d
      ```

  2. Start an `iex` console per node:

      ```console
      $ MIX_ENV=distributed iex --name node1@127.0.0.1 --erl "-config cluster/node1.sys.config" -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node2@127.0.0.1 --erl "-config cluster/node2.sys.config" -S mix
      ```

      ```console
      $ MIX_ENV=distributed iex --name node3@127.0.0.1 --erl "-config cluster/node3.sys.config" -S mix
      ```

The node specific `<node>.sys.config` files ensure the cluster is formed before starting your application, assuming this occurs within the 30 seconds timeout.

Once the cluster has formed, you can use your event store module from any node.

## Usage

Using the event store when run on a cluster of nodes is identical to single node usage. You can subscribe to a stream, or all streams, on one node and append events to the stream on another. The subscription will be notified of the appended events.

### Append events to a stream

```elixir
alias EventStore.EventData
alias MyApp.EventStore

defmodule ExampleEvent do
  defstruct [:key]
end

stream_uuid = EventStore.UUID.uuid4()

events = [
  %EventData{
    event_type: "Elixir.ExampleEvent",
    data: %ExampleEvent{key: "value"},
    metadata: %{user: "someuser@example.com"},
  }
]

:ok = EventStore.append_to_stream(stream_uuid, 0, events)
```

### Read all events

```elixir
alias MyApp.EventStore

recorded_events = EventStore.stream_all_forward() |> Enum.to_list()
```

### Subscribe to all Streams

```elixir
alias MyApp.EventStore

{:ok, subscription} = EventStore.subscribe_to_all_streams("example-subscription", self(), start_from: :origin)

receive do
  {:subscribed, ^subscription} ->
    IO.puts("Successfully subscribed to all streams")
end

receive do
  {:events, events} ->
    IO.puts("Received events: #{inspect(events)}")

    :ok = EventStore.ack(subscription, events)
end
```
