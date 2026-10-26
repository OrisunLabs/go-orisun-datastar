# Backend architecture patterns

This document describes the starter’s command consistency, event-store adapter and testing contracts, together with existing patterns that require coordinated conversion. The implementations live in `internal/eventstore`, `internal/eventstoretest` and the owning auth, profile and todo slices.

## Command consistency

Auth, profile and todo commands use `eventstore.SaverV2` and `SaveCommandEventsV2`. A `ConsistencyObservation` contains the exact query and position returned by one bounded authoritative read. Known criteria belong in one read; dependent criteria may require subsequent reads, all preserved independently in one atomic append. New UUID collision guards use a typed event criterion with `NoEventPosition`. Never merge observations into a maximum position or use repeated reads as a substitute for atomic checking.

OTP validation demonstrates dependent discovery: read the user and latest generated OTP together, then read validation for the discovered OTP. Both observations guard the append. A replacement OTP between reads or a competing validation before append must conflict. Command readers use `LatestRetriever`, which exposes no history scan. Orisun persists the write context, accessible via the occurrence’s `WriteID` and `GetWriteContext`. The application no longer builds `scope_event_ids` or passes fetched events to the saver.

Envelope selectors use `eventstore.EventTypeField` and `eventId` in application queries. The adapter translates them to `__eventType` and `__eventId` for reads, writes, subscriptions and indexes. Domain payload fields retain their own names. Latest-result overlap is deduplicated by full stored position, preserving different occurrences even if they share an event UUID.

## Verification

`internal/eventstoretest.New(t)` provisions real isolated in-memory Orisun SQLite through the same adapter used in production. It starts neither NATS nor PostgreSQL. Use it for stateful command behavior and CCC tests; fault injection may gate actual reads to introduce a competing real write. Todo tests cover repeat handling, ownership and concurrent deletion. OTP tests inspect persisted independent observations and reject concurrent replacement. Selector architecture tests enforce the shared application selector constant.

Run `task test`, `task vet`, `task build` and `git diff --check`. These do not replace live PostgreSQL provisioning, projection transaction or SSE/browser verification.

## Existing contracts requiring separate coordinated conversion

Command consistency is implemented across existing handlers. The coordinated conversions completed in this starter are:

- Todo, auth and profile event contracts are converted as described below. Any future event format change requires a coordinated update to constructors, decoders, consumers, queries, indexes and fixtures, with an explicit decision about existing stored history. Do not add mixed decoders or silently rewrite events.
- Command slices now own their criteria and context loaders across auth, todo and profile. Keep authority helpers private to the behavior; do not introduce configurable context frameworks.
- Todo bulk actions now use an explicit caller-owned selection with atomic event-authoritative validation and one append, as described below. Projection selection is a candidate snapshot, not a claim about all unprojected or future todos.
- Keep future event consumers behind strict per-event decoding, including nested protected PII. Plaintext validation requires the owning key and must complete before projection writes or other effects.
- Todo now uses a typed source-ordered fold and transaction-bound projection repository, described in `todo-projection-transactions.md`. Auth and profile use separate transaction-bound owners as documented in their projection contracts.
- Existing NATS KV session view-state remains the rendering path. Its CQRS split already follows the CQRS principle: commands acknowledge acceptance, projection notifications invalidate, and SSE reads and patches server-rendered fragments.

Database resets, reseeding and historical event conversions require explicit authorization.

## Todo event-contract conversion

All five todo events have typed payloads, checked constructors and strict decoders in `internal/features/todo/events.go`. `TodoCreated.todoCreatedId` equals the envelope event ID. Every todo event carries a required top-level `userId` as an immutable cross-domain entity reference. Creation is the todo root and omits `scopes`; subsequent facts carry only `scopes.todoCreatedId`. Sharing a physical event boundary does not turn an auth event into todo ancestry. The HTTP adapter supplies the authenticated user’s stable identity as `UserID`; todo commands do not depend on registration-event names. Todo projections store ownership in `todo_items.user_id`. The view/route `TodoID` and command inventory keys identify the entity using its creation event ID.

Decoders require exact fields and types, canonical nonzero UUIDs, matching payload/envelope identity, normalized nonempty titles of at most 160 bytes and canonical nonzero UTC instants. Constructors normalize input instants to UTC and return errors for invalid facts. Every retrieved todo authority is decoded before command decisions; every projection event is decoded before writing or publishing. Invalid timestamps are never replaced by the current time. Queries and indexes distinguish creation identity from descendant ancestry.

The real-Orisun lifecycle test exercises create, rename, complete, reopen and delete through public commands. Corrupt-history tests cover all five contracts, including unknown/missing/null fields, invalid timestamps, legacy scopes and envelope mismatch. Malformed events selected by command queries fail before append; the boundary-wide projection also rejects malformed routing fields that cannot match a targeted command query. Existing ownership, retry and concurrent deletion tests remain active.

This is a breaking todo event format and fresh-install projection schema change. Existing installations also require an explicit migration or recreation decision for the former `todo_items.user_registered_id` column. Existing singular-scope todo events are not upcast or rewritten: they fail strict decoding on replay. Existing projected todos are not automatically converted. Deployments with old todo history must explicitly choose a historical conversion strategy or authorize recreation of disposable development data before adopting this format. No database reset or reseed has been performed.

Command query ownership, bulk-action atomicity and projection transactions are completed in the sections below. Auth contracts are converted in the next section; profile contracts are converted below.

## Auth event-contract conversion

All twelve auth event types have checked constructors, typed strict decoders and a dispatch validator in `internal/auth/events.go`. Registration is a root and omits scopes. Auth descendants use plural `scopes` containing exact auth event backlinks. `AccountDeleted` keeps its deletion-request backlink only in scopes. Login auditing is a root with an explicit `user.kind` of `anonymous` or `registered`; only the registered variant contains `userRegisteredId`, and successful attempts require that variant.

Validation checks exact required fields, nested encrypted-value structure, canonical UUIDs and event-specific envelope identity, canonical UTC instants, six-digit OTPs, bcrypt password-hash structure, blind-index structure and duplicated blind-index agreement. Constructors propagate encryption failures. Decoding does not require decrypted subject keys, so validation remains possible after key erasure. Consumers that need plaintext return decryption/authentication errors before applying effects.

Commands validate selected authoritative occurrences before interpreting them. Reactions validate source events before invoking commands. Auth projections decode before key access or writes, require validated OTP ancestry instead of falling back to a history scan, and decrypt password-reset tokens before creating verification rows. Reset-token plaintext is checked against its sensitive blind index. Profile consumers of registration/name-change facts use auth’s updated decoders with profile-owned payloads covered by their separate conversion below. The registration reaction passes the stable auth identity rather than interpreting encrypted PII as strings.

Real-Orisun tests retain OTP consistency/concurrent-replacement checks and exercise registration and password-reset requests through public commands. Additional contract tests reject unknown, missing, null and legacy fields across every auth type before projection effects. The reset projection regression verifies the actual decrypted token and rejects tampered ciphertext before another write.

This is a breaking auth event-format change. Existing singular-scope auth history, empty registration scopes and the previous optional login user field are not accepted by the new decoders. Existing deployments need an explicit conversion or disposable-development recreation decision. No reset, reseed, event rewriting or index removal is automatic. Command query ownership and projection transactions are completed below.

## Profile event-contract conversion

Bio, avatar and header-image events are independent profile facts with event-specific IDs matching their envelopes, a top-level cross-domain `userId` and no auth-event scopes. Profile command intents accept `UserID`, translated from the authenticated identity at the HTTP edge. The profile read-model schema already uses `user_id`; its ownership index in Orisun is now `profile_owner` over `userId`. No source-registration event name appears in profile payloads.

Checked constructors and strict decoders enforce exact required fields, canonical nonzero UUIDs, canonical UTC instants, structured encrypted bio values and matching blind indexes. Image URLs must be root-relative paths or absolute HTTP(S) URLs, with no protocol-relative form, credentials, fragments, backslashes or whitespace/control characters. Empty bio values remain valid for clearing; decryption validates normalized plaintext, the 280-byte limit and blind-index agreement. PII authentication failures return errors rather than panicking or applying empty defaults.

Commands decode selected profile authority before decisions. Image commands validate the resulting event/URL before uploading bytes. Both owning projections decode profile facts before effects; minimal `userId` notifications follow successful persistence. Profile consumes only a read capability for subject PII keys. Auth registration/name-change subscriptions retain their own typed source decoders and translate those facts at the consumer edge.

Real-Orisun command tests cover bio round trips, unchanged retries, clearing, ciphertext tampering, concurrent bio updates, avatar/header ownership references and rejection of an invalid storage URL before upload. Malformed-contract tests cover all three event types and reject old scopes, unknown/missing/null fields and invalid owners before projection effects.

This is a breaking profile event format change. Old singular-scope profile history requires an explicit conversion or disposable-development recreation decision; no reset, reseed or historical rewrite has been performed. Command context/query ownership is completed in the next section. Bulk-action atomicity, projection transactions and SSE lifecycle verification are completed below.

## Command context and query ownership

Each auth, todo and profile behavior owns its private contexts, context loaders and query builders in its command-handler file. Shared query bundles have been removed. Account-deletion request and completion are separate behavior files; shared consumer ports remain in `ports.go`. Email command entrypoints delegate all authoritative reads, validation, decryption and context interpretation to their private loaders. Login auditing owns a save-only context containing the validated event and its collision observation.

Each command retains its existing criteria and batches of independent observations. Loaders preserve the exact position returned by each read rather than recomputing positions from fetched events. Each mutating behavior has one terminal `SaveCommandEventsV2` site. Pure title validation, MIME-extension mapping, event contracts, audit metadata and external-effect interfaces remain shareable. Orchestration helpers may call command handlers, but command handlers cannot delegate their persistence to helpers or other commands; todo bulk commands use the atomic selection contract described below.

`TestCommandAuthorityHelpersStayWithinOwningSlice` follows local helper calls and checks context ownership, rejects shared authority/query helpers and history scans, requires public handlers to delegate latest-event reads to loaders, and enforces one behavior per file and no multiple terminal save sites. Tests inspect persisted facts with independent queries instead of calling production command query builders.

Real-Orisun email tests verify recorded-send redelivery, failed-send retries, decoded recipients/content and persisted consistency contexts. Existing OTP replacement, todo deletion and profile bio race tests continue to exercise the original guards. This conversion changes code ownership without changing event contracts, query shapes, database schema or public command signatures. No reset or reseed was performed. Bulk-action atomicity, projection transactions and SSE lifecycle verification are completed below.

## Atomic todo bulk actions

`CompleteTodos`, `ReopenTodos` and `DeleteTodos` carry `Operations`, a complete map from selected todo creation IDs to caller-allocated UUIDv7 event/operation IDs. They are the sole command entry points for those intents; single-item routes supply a one-entry map. Deletion accepts either active or completed todos, while the clear-completed route adds `RequireCompleted` as a stricter precondition within that same validator. Callers retain this exact inventory for command retries. Empty selections are no-ops; invalid identifiers and duplicate operation IDs fail before storage access. HTTP delivery captures candidates from the owning read model and allocates IDs for that invocation. A separate HTTP invocation captures a new selection and allocates new operations; it is not an automatic retry of a previous inventory.

Each behavior owns its context and batches all known criteria into one `GetLatestByCriteria`: five latest lifecycle facts plus one expected outcome identity per selected todo. The envelope outcome criterion intentionally omits ownership filters so operation-ID reassignment cannot hide an existing outcome. Strict decoding checks outcome identity, todo and user. Existing matching outcomes satisfy exact retries even after later lifecycle changes. Retry receipts are excluded from the current-state fold.

Every unapplied member must exist, belong to the user, remain undeleted and match the action’s required current status. A stale selection rejects the entire command; no members are silently dropped or committed earlier. The complete eligible batch is saved at one terminal `SaveCommandEventsV2` site with the exact combined query and returned position. Any concurrent matching change makes the append conflict atomically. A selection covers its explicit todo inventory, not todos omitted by projection lag or created afterwards.

Consistency observations are persisted once by Orisun’s write context, not copied into each event’s metadata. Audit metadata remains per event; this avoids quadratic metadata growth for a batch. Existing `TodoCompleted`, `TodoReopened` and `TodoDeleted` contracts are unchanged. Retry lookup uses Orisun’s built-in envelope type/ID indexes; authority reads use `todo_created` and `todo_ancestry`.

Real-Orisun tests verify a shared write ID and complete stored observation, exact retries including historical outcomes, foreign-owner rejection, duplicate operation-ID rejection, reassignment conflicts, stale-status rejection and concurrent delete/reopen conflicts without partial effects. No migrations or data resets are required. Projection transaction ownership is completed below; SSE lifecycle verification is covered by [the stream lifecycle checks](sse-lifecycle.md).

## Batch-only todo command API

Every todo mutation now has exactly one batch-capable public command: `CreateTodosCommandHandler`, `RenameTodosCommandHandler`, `CompleteTodosCommandHandler`, `ReopenTodosCommandHandler` and `DeleteTodosCommandHandler`. The former singular command types, handlers and results have been removed. Single-item HTTP actions invoke these handlers with one item; no wrapper retains separate domain validation or persistence logic. Architecture guards enforce this public API.

Creation takes `Titles`, keyed by caller-allocated creation/event IDs. It validates and normalizes the full inventory, batches envelope-ID collision criteria, verifies existing roots for exact owner/title retries and appends all new roots once. Renaming takes `Operations`, keyed by todo creation ID, with each `TodoRename` carrying a caller-owned event ID, desired title and expected current title. All known authority/receipt reads are batched, all pending members are validated before one append, and receipt conflicts also check the requested title.

The rename form submits its original rendered title as `expectedTitle`. Unchanged titles remain no-ops. The expected-title guard prevents retrying such a no-op from overwriting an intervening rename; persisted outcomes still make exact retries succeed after later changes. Separate invocations allocate new intent identities. Event contracts remain unchanged.

Tests cover one- and multiple-item use through the same public handlers, creation collisions, stale expected titles, historical outcome retries, unchanged rename retries and concurrent create/delete conflicts with no partial writes. No database reset or migration was performed. Projection transaction ownership is completed below.

## Todo projection transaction ownership

Todo projection interfaces and typed facts live in `internal/features/todo/contracts.go`; `project.go` owns the ordered occurrence loop and storage-independent transition rules. `postgres_projection_repository.go` privately binds sqlc queries to the active owner transaction, rejects unbound/foreign/nested/expired contexts and commits once. Each occurrence reads replay evidence and current state, folds and persists its resulting state/occurrence before the next iteration. Notifications follow successful commit; replayed occurrences recover routing from stored evidence without decoding historical payloads again.

The canonical fresh-install todo schema retains completion and reopening facts, derives `completed` from their full positions and keeps occurrence provenance in `event_positions`. State, the row watermark and each occurrence’s evidence are persisted atomically. Existing installations require an explicit read-model schema recreation/migration decision; no data reset was run. The default event-handler callback still supplies singleton deliveries; the projector also accepts ordered multi-occurrence batches and is tested for partition equivalence. Source ordering between calls remains the serial subscriber’s responsibility.

Native PostgreSQL tests verify same-row read-after-write, batch/singleton/partition equivalence, retained lifecycle facts, full-position replay identity, rollback after a real constraint failure and owner transaction lifetime. Behavior tests verify post-commit publication and retry after notification failure without repeating effects. See [the self-contained contract and test instructions](todo-projection-transactions.md). The temporary PostgreSQL test server is separate from application and event databases. Auth projection transactions now follow the same owner boundary; see [auth projection transactions](auth-projection-transactions.md). Profile now uses the same transaction boundary; see [profile projection transactions](profile-projection-transactions.md). SSE lifecycle verification is covered by [the stream lifecycle checks](sse-lifecycle.md).

## Auth-owned profile facts

Auth projects avatar, bio and header-image events into its own user row using its existing watermark and transaction boundary. Server composition translates strict profile contracts into typed auth inputs, keeping packages independent and retaining ordinary `userId` references. All session/login reads query auth-owned tables. The separate image bridge and its direct write method/query are removed; auth emits routing-only profile invalidation after commit. See [auth projection transactions](auth-projection-transactions.md) for migration/replay requirements for existing installations.

## Terminal command persistence

A repository-wide syntax guard discovers command handlers from declarations and requires exactly one handler-owned `SaveCommandEventsV2` call in the common function body. It rejects legacy/raw saves, writes delegated to helpers or other commands, captures of save functions/methods, saves inside loops/closures/goroutines/defers and event construction after persistence. Negative fixture tests verify each prohibited shape; an atomic batch with an early retry return remains valid.

Password-reset request event construction and persistence now live directly in `RequestPasswordResetCommandHandler`. The user-lookup wrapper `ValidateEmailVerificationOTPForUser` remains orchestration and delegates to the typed OTP command. Neither change alters event contracts, query shapes, consistency observations, user-enumeration behavior or side-effect order. Existing real-Orisun command tests verify the behavior. The guard supplements command context/query ownership checks and covers both auth and feature command packages.

## Credential ownership

Password changes require the credential lookup's returned registered-user identity to match the command user. Account-deletion requests require both the registered-user identity and auth-user ID to match. These checks run before password verification, authority loading or persistence. Real-Orisun tests cover matching owners, mismatched identities, missing owners, incorrect passwords and lookup failures, and verify rejected commands append no facts. Password-change ownership is based on the stable registered-user identity; deletion additionally binds the auth account being deleted.

## Password-reset authority

A reset request requires a decoded registration event matching the resolved user's stable identity before generating a token or reading the subject key. Unknown-user lookup behavior is unchanged. Reset completion reconstructs the request's owner from its strict event contract, rejects a different resolved user and constructs the completion using the authoritative request identity. Existing completion facts must reference that same request and owner; inconsistent ancestry is an error rather than an ordinary already-completed result.

Both commands retain their existing query shapes and exact observed context positions. Real-Orisun regressions cover owner mismatches, missing registration/request, valid requests, reused resets and inconsistent completion ancestry. A gated concurrent-read test verifies two attempts cannot both complete the same reset. No event-format changes, migrations or data resets are introduced.

## Deletion completion identity

Deletion completion loads the exact request ID and its completion evidence, then strictly checks the registered-user identity, auth-user ID and request ancestry before any destructive effect or idempotent return. Data deletion, key destruction and the saved completion use identities from the validated request. A later request for the same user cannot replace the authority selected for an earlier delivery. Exact retries perform no repeated effects; inconsistent historical completion facts are rejected.

The bounded source query uses the existing `account_deletion_requested_id` and `deletion_ancestry` indexes, with `eventType` in both criteria. The exact returned observation position remains the save guard. Real-Orisun regressions cover mismatched command identities, malformed/missing requests, inconsistent completion owners, later requests and repeated delivery. The existing data-deletion/key-destruction/event-save order is unchanged; this slice does not make external effects atomic with event persistence. No schema migration or data reset is introduced.
