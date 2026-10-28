# Profile projection transactions

`project.go` owns typed registration/name, bio, avatar and header-image transitions. The SQLite adapter binds generated queries to an active owner transaction and rejects unbound, foreign, nested and expired contexts. `BEGIN IMMEDIATE` serializes writers before reads; the serial subscription owns source ordering between batches.

Each occurrence checks full-position replay evidence before decoding or key access, loads current state, folds its fields and saves state, watermark and evidence before the next occurrence. Later occurrences see earlier uncommitted writes. Failure rolls back the whole batch. Invalidation publishes only after commit and recovers routing from retained receipts on retry.

The canonical fresh-install schema is `migrations/001_init.sql`. JSON evidence uses SQLite `json_patch` and exact `json_type` membership. Primary keys serve state loads; JSON membership remains unindexed. No receipt-only table or extra source watermark is introduced. A historical unseen occurrence retains evidence without replacing newer fields. Sparse field-update profiles and explicit bio clearing retain their existing behavior.

Auth consumes profile events through its own owning projection. Account deletion removes the profile row and its evidence; permanent profile erasure is a separate conversion. This port preserves that policy.

Native tests run under ordinary `task test`, using isolated temporary SQLite files. They verify interleaved-user partition equivalence, own-write visibility, concurrent reader isolation, full-position replay, historical preservation, field clearing, real SQL/decryption rollback and transaction lifetime. Pure tests verify post-commit publication and retry. Application databases are never reset or seeded by verification.
