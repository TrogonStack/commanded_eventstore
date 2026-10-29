# Changelog

## [1.5.0](https://github.com/TrogonStack/commanded_eventstore/compare/v1.4.9...v1.5.0) (2026-10-29)


### Features

* **config:** Allow tuning pool pings, checkout retries and connection recycling ([#22](https://github.com/TrogonStack/commanded_eventstore/issues/22)) ([19072b4](https://github.com/TrogonStack/commanded_eventstore/commit/19072b4aec0da2eb6e1463f94ef3b9d1f3b427f9))
* **subscriptions:** Let operators tell which subscription a process serves in Observer and crash reports ([#95](https://github.com/TrogonStack/commanded_eventstore/issues/95)) ([c51477a](https://github.com/TrogonStack/commanded_eventstore/commit/c51477a05af7e3ebd5ff9bbf8ad0677c8f615ef1))


### Bug Fixes

* **config:** Make configured checkout retries apply to the queries they are meant to retry ([#58](https://github.com/TrogonStack/commanded_eventstore/issues/58)) ([eac69f8](https://github.com/TrogonStack/commanded_eventstore/commit/eac69f8658bd0b6ec399a2be0c4606fa6f440400))
* **migrations:** Add the lock generation even when upstream already recorded 1.5.0 ([#75](https://github.com/TrogonStack/commanded_eventstore/issues/75)) ([1450b5b](https://github.com/TrogonStack/commanded_eventstore/commit/1450b5b13da50c72751078f3421f3c849160dc8a))
* **migrations:** Keep the fork's migrations out of upstream's schema versions ([#89](https://github.com/TrogonStack/commanded_eventstore/issues/89)) ([ec067f3](https://github.com/TrogonStack/commanded_eventstore/commit/ec067f333ac4e07ed4c06ac2e56406a84ec34a12))
* **notifications:** Keep the listener from dropping notifications that arrive while a fast keepalive ping is in flight ([#105](https://github.com/TrogonStack/commanded_eventstore/issues/105)) ([bbc70c4](https://github.com/TrogonStack/commanded_eventstore/commit/bbc70c46db7cd78974dafc119529f05c7a3351fb))
* **streams:** Raise a meaningful error when a lazy stream read fails ([#76](https://github.com/TrogonStack/commanded_eventstore/issues/76)) ([788a578](https://github.com/TrogonStack/commanded_eventstore/commit/788a578fca7a56be43420f7f05a3ca35b01f1dc2))
* **subscriptions:** Keep a checkpoint that failed to write so the next one retries it ([#71](https://github.com/TrogonStack/commanded_eventstore/issues/71)) ([6fceaef](https://github.com/TrogonStack/commanded_eventstore/commit/6fceaef3e6757709fbbfe0c0f77f2f69fc6b2adc))
* **subscriptions:** Keep a failed repeated claim from giving up a lock the owner still holds ([#60](https://github.com/TrogonStack/commanded_eventstore/issues/60)) ([46533ec](https://github.com/TrogonStack/commanded_eventstore/commit/46533ec196ab05312b461c7e06026bb587f99e26))
* **subscriptions:** Keep a long table lock on subscriptions from dropping every lock on the node ([#59](https://github.com/TrogonStack/commanded_eventstore/issues/59)) ([6ed3bf2](https://github.com/TrogonStack/commanded_eventstore/commit/6ed3bf249544cadc14a5a23a56dae3135685ceb5))
* **subscriptions:** Keep a subscription alive and reading when a read fails while catching up ([#109](https://github.com/TrogonStack/commanded_eventstore/issues/109)) ([e902836](https://github.com/TrogonStack/commanded_eventstore/commit/e902836abe1fdce159a66879176babf40ca222e6))
* **subscriptions:** Keep retrying the unlock of a lock whose owner exited while the connection could not answer ([#61](https://github.com/TrogonStack/commanded_eventstore/issues/61)) ([17258ae](https://github.com/TrogonStack/commanded_eventstore/commit/17258ae3b27c3709edd0b77fdfbb254430cd10b3))
* **subscriptions:** Make caught up subscriptions read events appended while the notification connection was not listening ([#82](https://github.com/TrogonStack/commanded_eventstore/issues/82)) ([d3d3ec0](https://github.com/TrogonStack/commanded_eventstore/commit/d3d3ec05da1d7fc1fedd132c07acf61069703726))
* **subscriptions:** Release advisory locks when the lock connection reconnects on a new session ([#24](https://github.com/TrogonStack/commanded_eventstore/issues/24)) ([a0df6c4](https://github.com/TrogonStack/commanded_eventstore/commit/a0df6c43e353c410b5522dbcb510c73fbe7dee55))
* **subscriptions:** Report an unmigrated schema or missing grants from a node that loses every claim, and point upgraders to guides/Upgrades.md ([#110](https://github.com/TrogonStack/commanded_eventstore/issues/110)) ([c3b2fbf](https://github.com/TrogonStack/commanded_eventstore/commit/c3b2fbfdf2532ccdf2dc1e1c59918d5441161be3))
* **subscriptions:** Stop a checkpoint racing a delete from bringing the deleted subscription back ([#69](https://github.com/TrogonStack/commanded_eventstore/issues/69)) ([65993e2](https://github.com/TrogonStack/commanded_eventstore/commit/65993e2fb729b71aefd54a5c99349ad2b5e9d522))
* **subscriptions:** Stop a losing claim from writing the subscription row ([#90](https://github.com/TrogonStack/commanded_eventstore/issues/90)) ([3fa3533](https://github.com/TrogonStack/commanded_eventstore/commit/3fa3533896afcdf1d6746d8497401bbd6e9ef9a0))
* **subscriptions:** Stop a stale verification retry from polling alongside the pending one ([#67](https://github.com/TrogonStack/commanded_eventstore/issues/67)) ([959e09d](https://github.com/TrogonStack/commanded_eventstore/commit/959e09da2a9dd01cd9ebfc1e47eccdfcb1127ab5))
* **subscriptions:** Stop an acknowledgement from before a disconnect covering events the subscriber has not processed ([#72](https://github.com/TrogonStack/commanded_eventstore/issues/72)) ([7964d47](https://github.com/TrogonStack/commanded_eventstore/commit/7964d4708ab0b5b638459077d695f572a6dca0cd))
* **subscriptions:** Stop events heard while disconnected from multiplying retries against the database ([#70](https://github.com/TrogonStack/commanded_eventstore/issues/70)) ([383d9df](https://github.com/TrogonStack/commanded_eventstore/commit/383d9df63aa8a3542cef0cc90424881313f6f98e))
* **subscriptions:** Stop lock queries from waiting forever on a lock pool that went down ([#25](https://github.com/TrogonStack/commanded_eventstore/issues/25)) ([51b9550](https://github.com/TrogonStack/commanded_eventstore/commit/51b955026fd47a21d53dff9f57c78ae5417d5d13))
* **subscriptions:** Stop retrying a failed checkpoint write on every ack ([#91](https://github.com/TrogonStack/commanded_eventstore/issues/91)) ([74f5783](https://github.com/TrogonStack/commanded_eventstore/commit/74f578300c22e7a4baa15bfd9fd7b7e1ae1c45b3))
* **subscriptions:** Tell lock owners their lock is gone before the connection finishes closing ([#27](https://github.com/TrogonStack/commanded_eventstore/issues/27)) ([1f9d1f8](https://github.com/TrogonStack/commanded_eventstore/commit/1f9d1f8c291bbcca4a8171f362f7c1344753f0b0))
* **subscriptions:** Tell lock owners their session is gone when the call that hit it replies before the connection says so ([#87](https://github.com/TrogonStack/commanded_eventstore/issues/87)) ([600edd0](https://github.com/TrogonStack/commanded_eventstore/commit/600edd06de5a8013acde32878babf7a73c1e289d))
* **subscriptions:** Tell lock owners their session is gone without waiting for a stalled lock query to give up ([#62](https://github.com/TrogonStack/commanded_eventstore/issues/62)) ([446245b](https://github.com/TrogonStack/commanded_eventstore/commit/446245b55da19872f395589183cbd774a02e0716))
* **subscriptions:** Tell operators why subscriptions never start on an unmigrated schema or a lock role without grants ([#49](https://github.com/TrogonStack/commanded_eventstore/issues/49)) ([92d9848](https://github.com/TrogonStack/commanded_eventstore/commit/92d984822c907285700dd2ec868c2bb5b25e1a78))


### Miscellaneous

* **advisory-locks:** Stop keeping lock state that nothing ever reads ([#68](https://github.com/TrogonStack/commanded_eventstore/issues/68)) ([5fce7ad](https://github.com/TrogonStack/commanded_eventstore/commit/5fce7adc887e8ea14a6f72567e93fc316be4d5d9))
* **cluster:** Say server keepalives do not release a dead node's locks when PgBouncer holds the lock session ([#106](https://github.com/TrogonStack/commanded_eventstore/issues/106)) ([c66b654](https://github.com/TrogonStack/commanded_eventstore/commit/c66b654b46755818d6ef2084c5d0ebee15279ddc))
* **test:** Catch a server restart that reuses the backend pid of the lock session ([#64](https://github.com/TrogonStack/commanded_eventstore/issues/64)) ([a7968c0](https://github.com/TrogonStack/commanded_eventstore/commit/a7968c04fae9c2b3998c03e1d591b2bc0b2f177a))
* **test:** Catch a stale lock notice that drops the subscription's current lock ([#63](https://github.com/TrogonStack/commanded_eventstore/issues/63)) ([0802e7e](https://github.com/TrogonStack/commanded_eventstore/commit/0802e7e97f76045799f034926a51f529b5ebde8d))
* **test:** Catch lock regressions the lock tests let through ([#92](https://github.com/TrogonStack/commanded_eventstore/issues/92)) ([884c087](https://github.com/TrogonStack/commanded_eventstore/commit/884c08790bb09e046fed2def588b3bc07f716788))
* **test:** Catch migration regressions and make two test barriers reliable ([#94](https://github.com/TrogonStack/commanded_eventstore/issues/94)) ([2496d86](https://github.com/TrogonStack/commanded_eventstore/commit/2496d86b35ad7afc796ad9680d50f7ba61dd3c37))
* **test:** Catch subscription regressions the suite let through ([#93](https://github.com/TrogonStack/commanded_eventstore/issues/93)) ([5f97c1a](https://github.com/TrogonStack/commanded_eventstore/commit/5f97c1a4fd48484729f873534ef9990ec8bd9b71))
* **test:** Close the mutation and naming gaps in the test suite ([#111](https://github.com/TrogonStack/commanded_eventstore/issues/111)) ([c46b500](https://github.com/TrogonStack/commanded_eventstore/commit/c46b500c79b4553a01179c0f8cb33db2aaa7d766))
* **test:** Give selected events time to arrive when the database is slow to acknowledge checkpoints ([#98](https://github.com/TrogonStack/commanded_eventstore/issues/98)) ([d7c41a0](https://github.com/TrogonStack/commanded_eventstore/commit/d7c41a0b6ea6ad7b035c9d18d252fcbba9ddf935))
* **test:** Guard lock owners against losing a lock held by the connection made before the lock server started ([#65](https://github.com/TrogonStack/commanded_eventstore/issues/65)) ([92fe689](https://github.com/TrogonStack/commanded_eventstore/commit/92fe689a6f0543e84179852a4f898bfbd1d48586))
* **test:** Guard pending unlocks against verification retries that stop or multiply ([#66](https://github.com/TrogonStack/commanded_eventstore/issues/66)) ([26a554a](https://github.com/TrogonStack/commanded_eventstore/commit/26a554aee6ce3d3cc8a407416a4a2e45162f3e7f))
* **test:** Keep refused reconnect attempts from crashing the notification connection ([#107](https://github.com/TrogonStack/commanded_eventstore/issues/107)) ([be663f4](https://github.com/TrogonStack/commanded_eventstore/commit/be663f47ba17478760743061165f1f43f6be7769))
* **test:** Keep the tcp proxy from resetting connections a pool opens at once ([#97](https://github.com/TrogonStack/commanded_eventstore/issues/97)) ([4185a67](https://github.com/TrogonStack/commanded_eventstore/commit/4185a67e23ea64db2be25aed9f7b5e39530cb91d))
* **test:** Let the suite run against a database that is not on port 5432 ([#48](https://github.com/TrogonStack/commanded_eventstore/issues/48)) ([4fb5527](https://github.com/TrogonStack/commanded_eventstore/commit/4fb5527ba2b94a02ffeb684f91aa0f27725f3615))
* **test:** Stop lingering sessions from failing the migration database reset ([#96](https://github.com/TrogonStack/commanded_eventstore/issues/96)) ([7c2d6e9](https://github.com/TrogonStack/commanded_eventstore/commit/7c2d6e9e05756de19bdbddc19043852bb0f17e4d))
* **test:** Stop lock tests from failing without a regression ([#108](https://github.com/TrogonStack/commanded_eventstore/issues/108)) ([99a9765](https://github.com/TrogonStack/commanded_eventstore/commit/99a9765919ac906fdc02a3b611bca227a4ab27b1))
* **test:** Stop the fencing test appending before the store listens for new events ([#77](https://github.com/TrogonStack/commanded_eventstore/issues/77)) ([a2ce825](https://github.com/TrogonStack/commanded_eventstore/commit/a2ce8259dbf4e28df892d13fc0d5e63d25c33600))
* **test:** Stop timing bound tests failing under load ([#33](https://github.com/TrogonStack/commanded_eventstore/issues/33)) ([d1ca6d1](https://github.com/TrogonStack/commanded_eventstore/commit/d1ca6d1dc6c0af100e11e8053d78d9b87c959953))
* **upgrades:** Say the claim error is logged once so operators know not to wait for it to repeat ([#88](https://github.com/TrogonStack/commanded_eventstore/issues/88)) ([e761761](https://github.com/TrogonStack/commanded_eventstore/commit/e761761bf1cd33e4d88fdf7919365d24d78579cc))

## [1.4.9](https://github.com/TrogonStack/commanded_eventstore/compare/v1.4.8...v1.4.9) (2026-09-24)


### Bug Fixes

* **notifications:** Recover from a dead listener connection ([#11](https://github.com/TrogonStack/commanded_eventstore/issues/11)) ([a1268b5](https://github.com/TrogonStack/commanded_eventstore/commit/a1268b5441a6eb08f5b2cf62a9c632f2652a227a))
* **subscriptions:** Keep catching up when the checkpoint timer fires ([#10](https://github.com/TrogonStack/commanded_eventstore/issues/10)) ([23cbf8e](https://github.com/TrogonStack/commanded_eventstore/commit/23cbf8e7d5e841a50ce0ae674cd37f46de6b2799))
* **subscriptions:** Stop a deleted subscription from silently skipping its successor's events ([#21](https://github.com/TrogonStack/commanded_eventstore/issues/21)) ([10c61fc](https://github.com/TrogonStack/commanded_eventstore/commit/10c61fced14b72eff7c257235644e71e955c6ece))
* **subscriptions:** Stop a subscription delete from disconnecting its subscribers ([#20](https://github.com/TrogonStack/commanded_eventstore/issues/20)) ([43ac0b7](https://github.com/TrogonStack/commanded_eventstore/commit/43ac0b7b4884f56317ac0174fbd85cb33f8cb04b))


### Miscellaneous

* **ci:** Automate releases with release-please ([#7](https://github.com/TrogonStack/commanded_eventstore/issues/7)) ([051c22d](https://github.com/TrogonStack/commanded_eventstore/commit/051c22d7cb74b2c46d10607755ba4c92261017a7))
* **ci:** Restrict commit types to chore, feat and fix ([#19](https://github.com/TrogonStack/commanded_eventstore/issues/19)) ([fb651e2](https://github.com/TrogonStack/commanded_eventstore/commit/fb651e2e6d8e4102858c044a02cee8600d3d03ff))
* Claim copyright for the work done in this fork ([#8](https://github.com/TrogonStack/commanded_eventstore/issues/8)) ([2dcb046](https://github.com/TrogonStack/commanded_eventstore/commit/2dcb0461622cfcf5d9bd2bbebc5f0dded248907b))
* Clear shutdown noise and compiler warnings ([#12](https://github.com/TrogonStack/commanded_eventstore/issues/12)) ([01aea6a](https://github.com/TrogonStack/commanded_eventstore/commit/01aea6a2b477a7c1b90e097d0010cf95ac08a64b))
* Point self-referential links at this fork ([#6](https://github.com/TrogonStack/commanded_eventstore/issues/6)) ([44efa14](https://github.com/TrogonStack/commanded_eventstore/commit/44efa14db6dd4fb6cc0656337c4c8acf4a688480))
* Support only the last three Elixir and OTP releases ([#16](https://github.com/TrogonStack/commanded_eventstore/issues/16)) ([9215862](https://github.com/TrogonStack/commanded_eventstore/commit/92158627219f4279119ca5f316ec4896a6bbdf81))
* Sync with upstream commanded/eventstore ([f6a1fc6](https://github.com/TrogonStack/commanded_eventstore/commit/f6a1fc66754aa748cacdc808dd99ab1db646c1d8))
* Sync with upstream commanded/eventstore ([3532706](https://github.com/TrogonStack/commanded_eventstore/commit/3532706e0888017b53b480aeaa91a999d1076504))
* **test:** Stop the test environment from hiding compiler warnings ([#18](https://github.com/TrogonStack/commanded_eventstore/issues/18)) ([549dbe0](https://github.com/TrogonStack/commanded_eventstore/commit/549dbe030088767a0460d1524474ee99e685d0f3))

## v1.4.8

### Bug fixes

* Stream.stream_forward and Stream.stream_backward should handle gaps by @lachiemurray in https://github.com/commanded/eventstore/pull/297
* Use event count based on the batch by @yordis in https://github.com/commanded/eventstore/pull/302
* Remove status from sort_by options by @yordis in https://github.com/commanded/eventstore/pull/304
* Start pubsub supervisor before subscriptions supervisor by @drteeth in https://github.com/commanded/eventstore/pull/300

### Enhancements

* Add documentation for deleting streams by @quarterpi in https://github.com/commanded/eventstore/pull/305

## v1.4.7

### Enhancements

* Add support for overriding created_at timestamps for copy transform workflows by @Johnabell in https://github.com/commanded/eventstore/pull/282

### Bug fixes

* fix: merge session mode pool config with main config, preserving options by @gf3 in https://github.com/commanded/eventstore/pull/293

## v1.4.6

### Enhancements

* Expands CI test matrix to cover supported Elixir and OTP versions ([#291](https://github.com/commanded/eventstore/pull/291)).
* Bump version
* Updates changelog :)

## v1.4.5

### Enhancements

* Add a docker compose file for standing up a suitable development database ([#290](https://github.com/commanded/eventstore/pull/290)).

### Bug fixes

* `append_to_stream` can now properly append more than 1000 events again ([#289](https://github.com/commanded/eventstore/pull/289)).

## v1.4.4

### Bug fixes

* Handle `DBConnection.ConnectionError` errors when appending events to storage ([#261](https://github.com/commanded/eventstore/pull/261)).

## v1.4.3

### Enhancements

* Support Elixir v1.11 and later.
* Parse url with encoded hash in password ([#275](https://github.com/commanded/eventstore/pull/275)).
* Allow configuring the default database ([#277](https://github.com/commanded/eventstore/pull/277)).
* Fix Elixir `Logger.warn/2` warning deprecation message ([#278](https://github.com/commanded/eventstore/pull/278)).
* Quote schema names in SQL ([#266](https://github.com/commanded/eventstore/pull/266)).
* Fix unused read batch size ([#279](https://github.com/commanded/eventstore/pull/279)).

## v1.4.2

### Enhancements

* Allow `disconnect_on_error_codes` to be passed to the event store Postgrex connection ([#263](https://github.com/commanded/eventstore/pull/263)).
* Support all `Postgrex` and `DBConnection` options when configuring an event store ([#273](https://github.com/commanded/eventstore/pull/273)).

### Bug fixes

* Dialyzer error when calling `EventStore.subscribe/2` with `:name` option ([#268](https://github.com/commanded/eventstore/pull/268)).
* Terminate a monitored process only if there are no other processes registered to use it ([#272](https://github.com/commanded/eventstore/pull/272)).

## v1.4.1

### Enhancements

- Allow `:parameters` to be passed to the EventStore database connection ([#257](https://github.com/commanded/eventstore/pull/257)).
- Use configurable `:timeout` option for subscription database queries ([#259](https://github.com/commanded/eventstore/pull/259)).

## v1.4.0

- List running event store instances ([#244](https://github.com/commanded/eventstore/pull/244)).
- Paginate streams ([#246](https://github.com/commanded/eventstore/pull/246)).
- Add `stream_info/2` function ([#247](https://github.com/commanded/eventstore/pull/247)).
- Remove unmaintained `elixir_uuid` dependency ([#253](https://github.com/commanded/eventstore/pull/253)).
- Add option to use EventStore with PgBouncer ([#249](https://github.com/commanded/eventstore/pull/249)).

---

## v1.3.2

### Enhancements

- Add postgrex `socket_options` option ([#242](https://github.com/commanded/eventstore/pull/242)).

### Bug fixes

- Fix bug with subscriptions trigger in older Postgres versions ([#241](https://github.com/commanded/eventstore/pull/241)).

### Upgrading

This release includes a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

## v1.3.1

### Bug fixes

- Support running event store migrations when using a schema ([#239](https://github.com/commanded/eventstore/pull/239)).

## v1.3.0

- Improve performance of appending events under normal and degraded network conditions ([#230](https://github.com/commanded/eventstore/pull/230)).
- Subscription checkpoint tuning ([#237](https://github.com/commanded/eventstore/pull/237)).

## Bug fixes

- Fix bug with catch-up all streams subscription where the checkpoint is not committed for hard deleted streams ([#238](https://github.com/commanded/eventstore/pull/238)).

### Upgrading

This release requires a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

---

## v1.2.3

- Add `:configure` to postgrex connection options ([#233](https://github.com/commanded/eventstore/pull/233)).
- Use runtime configuration in Mix tasks ([#236](https://github.com/commanded/eventstore/pull/236)).

## v1.2.2

- Read stream and stream events backward ([#234](https://github.com/commanded/eventstore/pull/234)).

## v1.2.1

- Allow optional `event_id` to be included in `EventStore.EventData` struct ([#229](https://github.com/commanded/eventstore/pull/229)).
- Adds an option to supply an existing database connection or transaction to `EventStore` functions ([#231](https://github.com/commanded/eventstore/pull/231)).

## v1.2.0

### Enhancements

- Delete event stream ([#203](https://github.com/commanded/eventstore/pull/203)).
- Introduce `mix event_store.migrations` task to list migration status ([#207](https://github.com/commanded/eventstore/pull/207)).
- Remove distributed registry ([#210](https://github.com/commanded/eventstore/pull/210)).
- Hibernate subscription process after inactivity ([#214](https://github.com/commanded/eventstore/pull/214)).
- Runtime event store configuration ([#217](https://github.com/commanded/eventstore/pull/217)).
- Shared database connection pools ([#216](https://github.com/commanded/eventstore/pull/216)).
- Shared database connection for notifications ([#225](https://github.com/commanded/eventstore/pull/225)).
- Transient subscriptions ([#215](https://github.com/commanded/eventstore/pull/215))
- Improve resilience when database connection is unavailable ([#226](https://github.com/commanded/eventstore/pull/226)).

### Upgrading

This release requires a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

### Breaking changes

Usage of `EventStore.Tasks.Init` task to initialise an event store database has been changed as follows:

Previous usage:

```elixir
:ok = EventStore.Tasks.Init.exec(MyApp.EventStore, config, opts)
```

Usage now:

```elixir
:ok = EventStore.Tasks.Init.exec(config)
:ok = EventStore.Tasks.Init.exec(config, opts)
```

### Bug fixes

- Support appending events to a stream with `:any_version` concurrently ([#209](https://github.com/commanded/eventstore/pull/209)).

---

## v1.1.0

### Enhancements

- Support Postgres schemas ([#182](https://github.com/commanded/eventstore/pull/182)).
- Dynamic event store ([#184](https://github.com/commanded/eventstore/pull/184)).
- Add `timeout` option to config ([#189](https://github.com/commanded/eventstore/pull/189)).
- Namespace advisory lock to prevent clash with other applications ([#166](https://github.com/commanded/eventstore/issues/166)).
- Use database lock to prevent migrations from running concurrently ([#204](https://github.com/commanded/eventstore/pull/204)).

### Breaking changes

The following EventStore API functions have been changed where previously (in v1.0 and earlier) the last argument was an optional timeout (a non-negative integer or `:infinity`). This has been changed to be an optional Keyword list, which may include a timeout (e.g. `[timeout: 5_000]`). The `stream_forward` and `stream_all_forward` functions now also require the optional `read_batch_size` argument to be provided as part of the options Keyword list.

These changes were required to support dynamic event stores where an event store name can be included in the options to each function. If you did not provide a timeout to any of these functions then you will not need to make any changes to your code. See the example usages below for details.

- `EventStore.append_to_stream`
- `EventStore.link_to_stream`
- `EventStore.read_stream_forward`
- `EventStore.read_all_streams_forward`
- `EventStore.stream_forward`
- `EventStore.stream_all_forward`

Previous usage:

```elixir
EventStore.append_to_stream(stream_uuid, expected_version, events, timeout)
EventStore.link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout)
EventStore.read_stream_forward(stream_uuid, start_version, count, timeout)
EventStore.read_all_streams_forward(start_version, count, timeout)
EventStore.stream_forward(stream_uuid, start_version, read_batch_size, timeout)
EventStore.stream_all_forward(start_version, read_batch_size, timeout)
```

Usage now:

```elixir
EventStore.append_to_stream(stream_uuid, expected_version, events, timeout: timeout)
EventStore.link_to_stream(stream_uuid, expected_version, events_or_event_ids, timeout: timeout)
EventStore.read_stream_forward(stream_uuid, start_version, count, timeout: timeout)
EventStore.read_all_streams_forward(start_version, count, timeout: timeout)
EventStore.stream_forward(stream_uuid, start_version, read_batch_size: read_batch_size, timeout: timeout)
EventStore.stream_all_forward(start_version, read_batch_size: read_batch_size, timeout: timeout)
```

### Upgrading

This release requires a database migration to be run. Please read the [Upgrading an EventStore](https://hexdocs.pm/eventstore/upgrades.html) guide for details on how to migrate an existing database.

---

## v1.0.3

### Bug fixes

- Use event's stream version when appending events to a stream ([#202](https://github.com/commanded/eventstore/pull/202)).

## v1.0.2

#### Enhancements

- Prevent double supervision by starting / stopping supervisor manually ([#194](https://github.com/commanded/eventstore/pull/194)).
- Use `DynamicSupervisor` for subscriptions.

## v1.0.1

### Bug fixes

- Fix `EventStore.Registration.DistributedForwarder` state when running multiple nodes ([#186](https://github.com/commanded/eventstore/pull/186)).

## v1.0.0

### Enhancements

- Support multiple event stores ([#168](https://github.com/commanded/eventstore/pull/168)).
- Add support for `queue_target` and `queue_interval` database connection settings ([#172](https://github.com/commanded/eventstore/pull/172)).
- Add support for `created_at` values to be of type `NaiveDateTime` ([#175](https://github.com/commanded/eventstore/pull/175)).

### Bug fixes

- Fix function clause error on `DBConnection.ConnectionError` ([#167](https://github.com/commanded/eventstore/issues/167)).

### Upgrading

[Follow the upgrade guide](guides/upgrades/0.17-1.0.md) to define and use your own application specific event store].

---

## v0.17.0

### Enhancements

- SSL support including Mix tasks ([#161](https://github.com/commanded/eventstore/pull/161)).
- Use `timestamp with time zone` for timestamp fields ([#150](https://github.com/commanded/eventstore/pull/150)).

### Upgrading

Upgrade your existing EventStore database by running:

```console
mix event_store.migrate
```

**Note**: The migrate command is idempotent and can be safely run multiple times.

You can drop and recreate an EventStore database by running:

```console
mix do event_store.drop, event_store.create, event_store.init
```

---

## v0.16.2

### Bug fixes

- Fix issue with concurrent subscription partitioning ([#162](https://github.com/commanded/eventstore/pull/162)).
- Reliably start `EventStore.Notifications.Supervisor` on `:global` name clash ([#165](https://github.com/commanded/eventstore/pull/165)).

## v0.16.1

### Bug fixes

- Stop Postgrex database connection process in mix `event_store.init` and `event_store.migrate` tasks after use to prevent IEx shutdown when tasks are run together (as `mix do event_store.init, event_store.migrate`).
- Ensure the event store application doesn't crash when the database connection is lost ([#159](https://github.com/commanded/eventstore/pull/159)).

## v0.16.0

### Enhancements

- Add `:socket` and `:socket_dir` config options ([#132](https://github.com/commanded/eventstore/pull/132)).
- Rename `uuid` dependency to `elixir_uuid` ([#135](https://github.com/commanded/eventstore/pull/135)).
- Subscription concurrency ([#134](https://github.com/commanded/eventstore/pull/134)).
- Send `:subscribed` message to all subscribers connected to a subscription ([#136](https://github.com/commanded/eventstore/pull/136)).
- Update to `postgrex` v0.14 ([#143](https://github.com/commanded/eventstore/pull/143)).

### Breaking changes

- Replace `:poison` with `:jason` for JSON event data & metadata serialization ([#144](https://github.com/commanded/eventstore/pull/144)).

  To support this change you will need to derive the `Jason.Encoder` protocol for all of your events.

  This can be done by adding `@derive Jason.Encoder` before defining the struct in every event module.

  ```elixir
  defmodule Event1 do
    @derive Jason.Encoder
    defstruct [:id, :data]
  end
  ```

  Or using `Protocol.derive/2` for each event, as shown below.

  ```elixir
  require Protocol

  for event <- [Event1, Event2, Event3] do
    Protocol.derive(Jason.Encoder, event)
  end
  ```

---

## 0.15.1

### Enhancements

- Use a timeout of `:infinity` for the migration task (`mix event_store.migrate`) to allow database migration to run longer than the default 15 seconds.

### Bug fixes

- Socket closing causes the event store to never receive notifications ([#130](https://github.com/commanded/eventstore/pull/130)).
- Subscription with selector function should notify pending events after all filtered ([#131](https://github.com/commanded/eventstore/pull/131)).

## 0.15.0

- Support system environment variables for all config ([#115](https://github.com/commanded/eventstore/pull/115)).
- Allow subscriptions to filter the events they receive ([#114](https://github.com/commanded/eventstore/pull/114)).
- Allow callers to omit `event_type` when event data is a struct ([#118](https://github.com/commanded/eventstore/pull/118)).
- Remove dependency on `psql` for `event_store.create`, `event_store.init`, `event_store.migrate`, and `event_store.drop` mix tasks ([#117](https://github.com/commanded/eventstore/pull/117)).
- Supports query parameters in URL for database connection ([#119](https://github.com/commanded/eventstore/pull/119)).
- Improve typespecs and include Dialyzer in Travis CI build ([#121](https://github.com/commanded/eventstore/pull/121)).

---

## 0.14.0

- Add JSONB support ([#86](https://github.com/commanded/eventstore/pull/86)).
- Add `:ssl` and `:ssl_opts` config params ([#88](https://github.com/commanded/eventstore/pull/88)).
- Make `mix event_store.init` task do nothing if events table already exists ([#89](https://github.com/commanded/eventstore/pull/89)).
- Timeout issue when using `EventStore.read_stream_forward` ([#92](https://github.com/commanded/eventstore/pull/92)).
- Replace `:info` level logging with `:debug` ([#90](https://github.com/commanded/eventstore/issues/90)).
- Dealing better with Poison dependency ([#91](https://github.com/commanded/eventstore/issues/91)).
- Publish events directly to subscriptions ([#93](https://github.com/commanded/eventstore/pull/93)).
- Use PostgreSQL advisory locks to enforce only one subscription instance ([#98](https://github.com/commanded/eventstore/pull/98)).
- Remove stream process ([#99](https://github.com/commanded/eventstore/pull/99)).
- Use PostgreSQL's `NOTIFY` / `LISTEN` for event pub/sub ([#100](https://github.com/commanded/eventstore/pull/100)).
- Link existing events to another stream ([#103](https://github.com/commanded/eventstore/pull/103)).
- Subscription notification message once successfully subscribed ([#104](https://github.com/commanded/eventstore/pull/104)).
- Transient subscriptions ([#105](https://github.com/commanded/eventstore/pull/105)).
- Transient subscription event mapping function ([#108](https://github.com/commanded/eventstore/pull/108)).
- Turn EventStore `mix` tasks into generic tasks for use with Distillery during deployment ([#111](https://github.com/commanded/eventstore/pull/111)).

### Upgrading

Upgrade your existing EventStore database by running:

```console
mix event_store.migrate
```

You can drop and recreate an EventStore database by running:

```console
mix do event_store.drop, event_store.create, event_store.init
```

---

## v0.13.2

### Bug fixes

- Use `Supervisor.child_spec` with an explicit `id` for Registry processes to support Elixir v1.5.0 and v1.5.1 ([v1.5.2](https://github.com/elixir-lang/elixir/blob/v1.5/CHANGELOG.md#v152-2017-09-29) contains a fix for this issue).

## v0.13.1

### Bug fixes

- EventStore migrate mix task read migration SQL scripts from app dir (`Application.app_dir(:eventstore)`).

## v0.13.0

### Enhancements

- Use a UUID field for the `event_id` column, rename existing field to `event_number` ([#75](https://github.com/commanded/eventstore/issues/75)).
- Use `uuid` data type for event `correlation_id` and `causation_id` ([#57](https://github.com/commanded/eventstore/pull/57)).
- Mix task to migrate an existing EventStore database (`mix event_store.migrate`).

### Bug fixes

- Append to stream is limited to 7,281 events in a single request ([#77](https://github.com/commanded/eventstore/issues/77)).

### Upgrading

Upgrade your existing EventStore database by running: `mix event_store.migrate`

Or you can drop and recreate the EventStore database by running: `mix do event_store.drop, event_store.create, event_store.init`

---

## v0.12.1

### Bug fixes

- Publisher only notifies first pending event batch ([#81](https://github.com/commanded/eventstore/issues/81)).

## v0.12.0

### Enhancements

- Allow optimistic concurrency check on write to be optional ([#31](https://github.com/commanded/eventstore/issues/31)).

### Bug fixes

- Fix issue where subscription doesn't immediately receive events published while transitioning between catch-up and subscribed. Any missed events would be noticed and replayed upon next event publish.

---

## v0.11.0

### Enhancements

- Support for running on a cluster of nodes using [Swarm](https://hex.pm/packages/swarm) for process distribution ([#53](https://github.com/commanded/eventstore/issues/53)).

- Add `stream_version` column to `streams` table. It is used for stream info querying and optimistic concurrency checks, instead of querying the `events` table.

### Upgrading

Run the schema migration [v0.11.0.sql](priv/event_store/migrations/v0.11.0.sql) script against your event store database.

---

## v0.10.1

### Bug fixes

- Fix for ack of last seen event in stream subscription ([#66](https://github.com/commanded/eventstore/pull/66)).

## v0.10.0

### Enhancements

- Writer per event stream ([#55](https://github.com/commanded/eventstore/issues/55)).

  You **must** run the schema migration [v0.10.0.sql](priv/event_store/migrations/v0.10.0.sql) script against your event store database.

- Use [DBConnection](https://hexdocs.pm/db_connection/DBConnection.html)'s built in support for connection pools (using poolboy).

---

## v0.9.0

### Enhancements

- Adds `causation_id` alongside `correlation_id` for events ([#48](https://github.com/commanded/eventstore/pull/48)).

  To migrate an existing event store database execute [v0.9.0.sql](priv/event_store/migrations/v0.9.0.sql) script.

- Allow single stream, and all streams, subscriptions to provide a mapper function that maps every received event before sending to the subscriber.

  ```elixir
  EventStore.subscribe_to_stream(stream_uuid, "subscription", subscriber, mapper: fn event -> event.data end)
  ```

- Subscribers now receive an `{:events, events}` tuple and should acknowledge receipt by: `EventStore.ack(subscription, events)`

---

## v0.8.1

### Enhancements

- Add Access functions to `EventStore.EventData` and `EventStore.RecordedEvent` modules ([#37](https://github.com/commanded/eventstore/pull/37)).
- Allow database connection URL to be provided as a system variable ([#39](https://github.com/commanded/eventstore/pull/39)).

### Bug fixes

- Writer not parsing database connection URL from config ([#38](https://github.com/commanded/eventstore/pull/38/files)).

## v0.8.0

### Enhancements

- Stream events from a single stream forward.

---

## v0.7.4

### Enhancements

- Subscriptions use Elixir [streams](https://hexdocs.pm/elixir/Stream.html) to read events when catching up.

## v0.7.3

### Enhancements

- Upgrade `fsm` dependency to v0.3.0 to remove Elixir 1.4 compiler warnings.

## v0.7.2

### Enhancements

- Stream all events forward ([#34](https://github.com/commanded/eventstore/issues/34)).

## v0.7.1

### Enhancements

- Allow snapshots to be deleted ([#26](https://github.com/commanded/eventstore/issues/26)).

## v0.7.0

### Enhancements

- Subscribe to a single stream, or all streams, from a specified start position ([#17](https://github.com/commanded/eventstore/issues/17)).

---

## v0.6.2

### Bug fixes

- Subscriptions that are at max capacity should wait until all pending events have been acknowledged by the subscriber being catching up with any unseen events.

## v0.6.1

### Enhancements

- Use IO lists to build insert events SQL statement ([#23](https://github.com/commanded/eventstore/issues/23)).

## v0.6.0

### Enhancements

- Use `NaiveDateTime` for each recorded event's `created_at` property.

---

## v0.5.2

### Enhancements

- Provide typespecs for the public API ([#16](https://github.com/commanded/eventstore/issues/16))
- Fix compilation warnings in mix database task ([#14](https://github.com/commanded/eventstore/issues/14))

### Bug fixes

- Read stream forward does not use count to limit returned events ([#10](https://github.com/commanded/eventstore/issues/10))

## v0.5.0

### Enhancements

- Ack handled events in subscribers ([#18](https://github.com/commanded/eventstore/issues/18)).
- Buffer events between publisher and subscriber ([#19](https://github.com/commanded/eventstore/issues/19)).
