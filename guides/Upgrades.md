# Upgrading an EventStore

Versions that need a schema migration are listed below. The [CHANGELOG](https://github.com/TrogonStack/commanded_eventstore/blob/main/CHANGELOG.md) of this fork and the [upstream CHANGELOG](https://github.com/commanded/eventstore/blob/master/CHANGELOG.md) for versions before the fork describe what changed in each release.

## Upgrading to 1.5.0

1.5.0 adds a `lock_generation` column to the `subscriptions` table, which fences the progress writes of a node that lost a subscription's lock. Run `mix event_store.migrate` (or `EventStore.Tasks.Migrate.exec/2` from a release) before starting any node on 1.5.0. EventStore does not migrate on start: a 1.5.0 node on an unmigrated database starts, but none of its persistent subscriptions ever subscribe, and it logs an error naming the migration once, then keeps retrying without logging again until the schema is migrated.

Subscription locks are now taken with an `UPDATE` on the `subscriptions` table over the session mode connection. When `session_mode_pool` (or `session_mode_url`) connects as a different role from the main pool, grant that role `SELECT` and `UPDATE` on `subscriptions` in the event store's schema:

```sql
GRANT SELECT, UPDATE ON "public".subscriptions TO session_mode_role;
```

Without these grants, persistent subscriptions never subscribe and the node logs an error naming the missing privileges once, then keeps retrying without logging again.

## Migrate before rolling nodes

Run the migration before starting nodes on the new version. A node on a version that expects a newer schema cannot acquire subscription locks against an unmigrated database and keeps retrying until the migration has run. Nodes on the previous version keep working against the migrated schema once the migration is in place.

While versions are mixed, the lock generation fencing described in [Running on a cluster of nodes](Cluster.md) only holds between nodes that run 1.5.0 or later. A node on 1.4.x neither moves the generation on when it takes a lock nor checks it when it acks, so during the rollout:

- a 1.4.x node that lost its lock can still ack, and its write can move the new owner's position backwards;
- a 1.5.0 node that lost its lock to a 1.4.x node is not fenced, because the generation it holds is still current.

Both need a node that keeps running after its lock session ended, such as one cut off from the database, while another node takes the lock. A node that shuts down cleanly stops its subscriptions before its locks are released, so restart nodes one at a time with a clean shutdown and keep the mixed-version window short. When a subscription must never record a position from a stale node, stop every node and start them all on the new version instead.

## Migrating a database with `mix`

You can upgrade an existing EventStore database using the following mix task:

```shell
mix event_store.migrate
```

The above command expects you to have configured the event store(s) for your application, as shown in the following example:

```elixir
# config/config.exs
config :my_app, event_stores: [MyApp.EventStore]
```

Or use `mix event_store.migrate -e MyApp.EventStore` to specify an event store as an argument.

Run this command each time you need to upgrade. The command is idempotent and will only run pending migrations if there are any and can be safely run multiple times. It can also be run concurrently as a database lock is used to ensure only one migration is run at at time.

It is *always* worth taking a full backup of the EventStore database before applying an upgrade.

Creating an EventStore database, using the `mix event_store.create` task, will always use the latest database schema.

## Migrating a database using an Elixir release

If you're using an Elixir release built with [mix release](https://hexdocs.pm/mix/Mix.Tasks.Release.html) you won't have `mix` available and won't be able to run the above command to migrate the database.

Instead you can use the [EventStore.Tasks.Migrate](https://github.com/commanded/eventstore/blob/master/lib/event_store/tasks/migrate.ex) task module and [running one-off commands](https://hexdocs.pm/mix/Mix.Tasks.Release.html#module-one-off-commands-eval-and-rpc) supported by Mix release, using a helper module defined like this:

```elixir
defmodule MyApp.ReleaseTasks do
  def migrate_event_store do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ssl)

    :ok = Application.load(:my_app)

    config = MyApp.EventStore.config()

    :ok = EventStore.Tasks.Migrate.exec(config, [])    
  end
end
```

Then run:

```shell
bin/RELEASE_NAME eval "MyApp.ReleaseTasks.migrate_event_store()"
```
