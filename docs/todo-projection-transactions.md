# Todo projection transactions

`contracts.go` defines the owner-specific repository, state, lifecycle facts and occurrence identity. `project.go` interprets events without importing a database driver or generated queries. The PostgreSQL repository owns transaction lifetime and typed SQL mapping; the projector owns application order and decisions.

For an ordered occurrence sequence, `ProjectBatch` opens one `WithinTransaction` block. For each occurrence, using the block’s context, it reads persisted full-position evidence, loads current state, applies one typed change and saves the resulting state and occurrence before moving to the next event. Later events therefore see earlier uncommitted changes to the same row. Any failure rolls back all state and provenance from the block. Source positions must increase strictly; the source boundary is implicit in this application’s fixed todo projection partition.

The repository serializes owner writes with a transaction-scoped advisory lock. Its context binding checks the repository instance and active lifetime; unbound, foreign, nested and escaped transaction contexts fail without a pool fallback. The lock provides exclusion, not ordering between independently scheduled batches. The existing serial subscriber owns that ordering and currently delivers singleton callbacks. The projector supports larger ordered batches with identical logical results.

State retains the latest completion and reopening facts, including their source positions, after reopening. PostgreSQL derives the display `completed` column from those positions, matching the pure Go fold. Native constraints require complete lifecycle facts and prevent future/negative lifecycle positions. The last applied commit/prepare pair guards replacement state; `event_positions` retains every occurrence’s event type and full position, without duplicating envelope IDs. State, watermark and one new provenance entry are saved by the same sqlc upsert.

Replay looks up persisted occurrence evidence before decoding historical payloads. It can recover the user routing identity from that evidence, so a notification failure after commit can retry publication without repeating state transitions. User IDs are deduplicated for publication after the transaction commits. NATS carries only `userId`, and subscribers reload the owning read model. The event-handler infrastructure advances its separate subscription checkpoint only after projection and publication succeed.

`todo_items` has B-tree indexes for actual user/list predicates and the full-position lower bound used by replay lookup. The watermark index bounds candidate rows; it does not index arbitrary JSON provenance membership. No GIN index or separate receipt table is added. SQL stays in `queries/todo.sql` and is generated with `task sqlc`.

## Verification

Run `task test`, `task vet`, `task build` and `git diff --check`. Default tests use consumer fakes for behavior and real Orisun SQLite for commands. Native PostgreSQL projection tests require a disposable database:

```sh
TEST_DATABASE_URL='postgres://user:password@localhost/test_database?sslmode=disable' task test
```

Tests create uniquely named schemas, apply the owning canonical todo schema and remove only those schemas. They verify native same-row visibility within a transaction, isolation before commit, rollback after an actual constraint failure, singleton/batch/partition equivalence, full-position occurrence identity, redelivery without historical decoding, retained lifecycle facts and transaction-context lifetime. They skip explicitly when `TEST_DATABASE_URL` is unset.

This changes the fresh-install read-model schema. Existing todo projections require an explicit schema migration or recreation/replay decision. No automatic reset, reseed or event-history rewrite is included. Auth/profile projection transactions, cross-feature account-data cleanup and live SSE lifecycle verification are outside this slice.
