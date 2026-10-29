# Backend architecture patterns

This document describes the starter’s command consistency, event-store adapter and testing contracts, together with existing patterns that require coordinated conversion. The implementations live in `internal/eventstore`, `internal/eventstoretest` and the owning auth, profile and todo slices.

## Command consistency

Auth, profile and todo commands use `eventstore.SaverV2` and `SaveCommandEventsV2`. A `ConsistencyObservation` contains the exact query and position returned by one bounded authoritative read. Known criteria belong in one read; dependent criteria may require subsequent reads, all preserved independently in one atomic append. Collision guards include typed event criteria in an authoritative read; an empty result supplies its returned `NoEventPosition`, rather than an invented observation. Never merge observations into a maximum position or use repeated reads as a substitute for atomic checking.

OTP validation demonstrates dependent discovery: read the pending registration and latest generated OTP together, then read validation for the discovered OTP. Both observations guard the append. A replacement OTP between reads or a competing validation before append must conflict. Command readers use `LatestRetriever`, which exposes no history scan. Orisun persists the write context, accessible via the occurrence’s `WriteID` and `GetWriteContext`. The application no longer builds `scope_event_ids` or passes fetched events to the saver.

Envelope selectors use `eventstore.EventTypeField` and `eventId` in application queries. The adapter translates them to `__eventType` and `__eventId` for reads, writes, subscriptions and indexes. Domain payload fields retain their own names. Latest-result overlap is deduplicated by full stored position, preserving different occurrences even if they share an event UUID.

## Verification

`internal/eventstoretest.New(t)` provisions real isolated in-memory Orisun SQLite through the same adapter used in production. It starts neither NATS nor SQLite. Use it for stateful command behavior and CCC tests; fault injection may gate actual reads to introduce a competing real write. Todo tests cover repeat handling, ownership and concurrent deletion. OTP tests inspect persisted independent observations and reject concurrent replacement. Selector architecture tests enforce the shared application selector constant.

Run `task test`, `task vet`, `task build` and `git diff --check`. These do not replace embedded SQLite startup, projection transaction or SSE/browser verification.

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

All auth event types—including registration workflow contracts—have their names, payloads, constructors, strict decoders and dispatch validation in `internal/auth/events.go`. `UserRegistrationStarted` and `UserRegistered` are separate roots and omit scopes. Auth descendants use plural `scopes` containing exact authoritative event backlinks. `AccountDeleted` scopes only the accepted user; `AccountDeletionCompleted` carries the request/effective-deletion backlinks. Login auditing is a root with an explicit `user.kind` of `anonymous` or `registered`; only the registered variant contains `userRegisteredId`, and successful attempts require that variant.

Validation checks exact required fields, nested encrypted-value structure, canonical UUIDs and event-specific envelope identity, canonical UTC instants, six-digit OTPs, bcrypt password-hash structure, blind-index structure and duplicated blind-index agreement. Constructors propagate encryption failures. Decoding does not require decrypted subject keys, so validation remains possible after key erasure. Consumers that need plaintext return decryption/authentication errors before applying effects.

Commands validate selected authoritative occurrences before interpreting them. Reactions validate source events before invoking commands. Auth projections decode before key access or writes, consume the complete accepted registration independently of OTP history, and decrypt password-reset tokens before creating verification rows. Reset-token plaintext is checked against its sensitive blind index. Profile consumers of registration/name-change facts use auth’s updated decoders with profile-owned payloads covered by their separate conversion below. The registration reaction passes the stable auth identity rather than interpreting encrypted PII as strings.

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

Todo projection interfaces and typed facts live in `internal/features/todo/contracts.go`; `project.go` owns the ordered occurrence loop and storage-independent transition rules. `sqlite_projection_repository.go` privately binds sqlc queries to the active owner transaction, rejects unbound/foreign/nested/expired contexts and commits once. Each occurrence reads replay evidence and current state, folds and persists its resulting state/occurrence before the next iteration. Notifications follow successful commit; replayed occurrences recover routing from stored evidence without decoding historical payloads again.

The canonical fresh-install todo schema retains completion and reopening facts, derives `completed` from their full positions and keeps occurrence provenance in `event_positions`. State, the row watermark and each occurrence’s evidence are persisted atomically. Existing installations require an explicit read-model schema recreation/migration decision; no data reset was run. The default event-handler callback still supplies singleton deliveries; the projector also accepts ordered multi-occurrence batches and is tested for partition equivalence. Source ordering between calls remains the serial subscriber’s responsibility.

Native SQLite tests verify same-row read-after-write, batch/singleton/partition equivalence, retained lifecycle facts, full-position replay identity, rollback after a real constraint failure and owner transaction lifetime. Behavior tests verify post-commit publication and retry after notification failure without repeating effects. See [the self-contained contract and test instructions](todo-projection-transactions.md). The temporary SQLite test databases are separate from application and event databases. Auth projection transactions now follow the same owner boundary; see [auth projection transactions](auth-projection-transactions.md). Profile now uses the same transaction boundary; see [profile projection transactions](profile-projection-transactions.md). SSE lifecycle verification is covered by [the stream lifecycle checks](sse-lifecycle.md).

## Auth-owned profile facts

Auth projects avatar, bio and header-image events into its own user row using its existing watermark and transaction boundary. Server composition translates strict profile contracts into typed auth inputs, keeping packages independent and retaining ordinary `userId` references. All session/login reads query auth-owned tables. The separate image bridge and its direct write method/query are removed; auth emits routing-only profile invalidation after commit. See [auth projection transactions](auth-projection-transactions.md) for migration/replay requirements for existing installations.

## Terminal command persistence

A repository-wide syntax guard discovers command handlers from declarations and requires exactly one handler-owned `SaveCommandEventsV2` call in the common function body. It rejects legacy/raw saves, writes delegated to helpers or other commands, captures of save functions/methods, saves inside loops/closures/goroutines/defers and event construction after persistence. Negative fixture tests verify each prohibited shape; an atomic batch with an early retry return remains valid.

Password-reset request event construction and persistence now live directly in `RequestPasswordResetCommandHandler`. The registration workflow orchestrates typed OTP validation without querying accepted-user projections. Neither change alters event contracts, query shapes, consistency observations, user-enumeration behavior or side-effect order. Existing real-Orisun command tests verify the behavior. The guard supplements command context/query ownership checks and covers both auth and feature command packages.

## Credential ownership

Account-deletion requests take explicit authenticated registered-user and auth-user IDs; their context binds both to accepted registration. In this starter, the auth-user ID is the accepted registration identity. Password changes take the authenticated registered-user ID and password inputs. Neither command depends on a credential reader or UI user object. Both verify submitted passwords against authoritative event state. Rejected inputs append no facts.

## Password-reset authority

Reset requests discover accepted registration by normalized email blind index, then observe effective account deletion using the discovered user ID. Unknown, pending and deleted users return an empty domain result. Before saving, the command decrypts the registration email with the read-only subject-key port and checks both normalized plaintext and its blind index. Both exact source observations guard the append; a concurrent deletion invalidates it. No user/credential projection is consulted, and the public acknowledgement remains generic.

Reset authorization uses only its narrow token-discovery port, which returns request/user IDs. The command validates those IDs against strict request/registration events, checks token hash, expiry, deletion and source ordering, and freezes its domain operation. Completion independently proves the exact authorized password change. Those later stages retain their separate command boundaries.

## Account deletion request, effect and completion

`AccountDeletionRequested` freezes complete deletion input and a distinct `operationId`. A new request observes accepted-user/password authority and existing deletion facts together, then takes a separate collision observation for its freshly allocated request and domain identities. Exact retries reuse the committed operation.

The request reaction invokes only `DeleteAccountCommandHandler`. This independent domain command accepts complete trusted input, checks registration and content-bound effective-operation identity, performs data deletion followed by subject-key destruction, and saves `AccountDeleted`. The effective fact scopes only the accepted user; it contains no deletion-request reference. Direct backend deletion uses the same command without a workflow.

The `AccountDeleted` reaction invokes only `CompleteAccountDeletionCommandHandler`. That command correlates the request by operation ID and proves matching user/auth identities and full-position ordering after authorization. It emits `AccountDeletionCompleted` referencing the exact request and effective deletion. Direct domain deletions have no workflow to complete. Completion performs no cleanup and can run after all auth rows and subject keys are gone.

Known domain operations skip external effects. Cleanup and event saving retain their existing order and at-least-once retry behavior: a failure after cleanup but before the domain append can repeat idempotent cleanup. Workflow completion retries never repeat it. This is a fresh-install event-contract change; no history conversion, database reset or reseed is performed. Native SQLite checks use isolated temporary database files and verify removal of only the selected account’s data/key, continued login for another account, and completion after key erasure.

## Authoritative passwords for mutations

Password changes and account-deletion requests need no credential projection; HTTP supplies authenticated IDs, and commands bind them to accepted event authority. Their behavior-owned context loaders reconstruct the effective hash from strict `UserRegistered`, `PasswordChanged` events. Registration supplies the initial hash; the latest effective password-change fact by full commit/prepare position supplies the current hash. Both commands require registration, reject mismatched owners or malformed facts and verify the submitted password against this authoritative state. A lagging credential projection cannot keep an old password valid or reject a newly accepted one for these mutations.

Each loader batches all known criteria into one latest-event read and retains its exact query and returned context position for the terminal append. Both loaders observe registration and effective password changes. Password changes also observe effective account deletion and reject deleted accounts; deletion requests observe their own deletion workflow. The existing `auth_user_ancestry` index covers their event types and `scopes.userRegisteredId` predicate; registration uses its existing identity index. No new index or event format is required.

Real-Orisun regressions cover initial credentials, projection lag after changes/resets, both change/reset orderings, foreign-user facts, missing registration and malformed reset history. Injecting an effective password update after the authority read makes both commands fail atomically with no additional mutation. Session/login authentication continues to use its existing read-model path; this slice changes password-authorized mutations only. No schema migration or data reset is introduced.

## Email retry after PII erasure

OTP and reset-email context loaders strictly decode their source and sent-receipt contracts and check matching identities before key lookup. A recorded send returns without reading the subject key, decrypting PII, sending another email or appending another receipt. OTP generations must belong to the command's pending registration, including on recorded-send retries. Unsent messages still require their source, recipient authority and a valid subject key; malformed source/receipt contracts remain errors even when a sent receipt exists.

The commands and email reactions consume only the subject-key read capability. Existing bounded query shapes, exact consistency observations and one terminal V2 save are retained. Real-Orisun regressions cover replay after actual key destruction, unavailable key services, malformed source/receipt history, unsent messages without keys and foreign-owner OTPs. This change does not provide exactly-once external delivery: sending followed by a failed receipt append retains the existing at-least-once retry behavior. No event-format change, migration or data reset is introduced.

Deletion-request retries bind any existing effective deletion to the request’s frozen operation and auth identity. The separate completion command validates the exact request/effect/receipt relationship; malformed or inconsistent evidence never produces a success fact.

## OTP validation context identity

OTP validation strictly decodes the selected generation and registration facts and binds them to the pending registration. The dependent validation lookup remains keyed by the discovered OTP ID; its facts must reference both that OTP and the same pending registration. A foreign-owner validation is rejected as inconsistent context rather than treated as a legitimate prior validation. Validation events use the checked context identity.

The latest OTP is selected by full source position, with invalid contracts/timestamps returned as errors rather than silently skipped. The two independent observations, query shapes, source indexes and atomic append remain unchanged. Real-Orisun regressions cover valid/current OTPs, previous-generation validation, foreign validation ownership, missing authority, incorrect/expired codes and malformed generation. Existing persisted-observation and concurrent-replacement tests remain active. No migration, event-format change or data reset is introduced.

## Registration proposal and acceptance

See [registration workflow](registration-workflow.md). Signup records `UserRegistrationStarted`; OTP facts authorize that pending workflow. Only the independent domain command emits `UserRegistered`, carrying the full accepted details and verification time. Auth and profile projections do not need pending registration or OTP history.

## Password reset authorization and domain effect

Token submission records `PasswordResetAuthorized`, freezing the password hash and a separately allocated domain operation ID after checking authoritative token ownership, hash and expiry. Retries reuse that authorization. The independent `SetPasswordCommandHandler` accepts complete trusted input and emits `PasswordChanged` scoped only to the accepted user. It has no reset-workflow dependencies.

HTTP acknowledges authorization. A checkpointed authorization reaction invokes only `SetPasswordCommandHandler`; a separate `PasswordChanged` reaction invokes only `CompletePasswordResetCommandHandler`. `PasswordResetCompleted` references the authorization and exact effective change, with no password hash. Completion proves matching owner/hash and source position after authorization. A historical retry never reapplies an older password after a newer direct change. Password-authorized commands and credential projections read effective facts only. This is a fresh-install event-contract change; no history conversion or data reset is performed.

Registration uses the same two-reaction boundary: OTP validation triggers only the independent registration command; `UserRegistered` triggers only completion. Reactions translate source input and attach metadata. Command handlers decide authority, completion applicability and idempotency. No finalizer loads workflow status to choose commands, and HTTP does not execute subsequent stages. Frozen registration input remains protected through execution, so historical delivery reaches domain idempotency without subject-key access.

## Name-command authority

`UpdateUserNameCommand` carries the accepted user ID and requested name; it does not carry a UI user object. HTTP supplies the authenticated identity. The command’s private context strictly decodes and binds selected registration/name facts before key access, checks source ordering, and reconstructs the current name from the latest effective fact. Registration names and username provide the initial fallback. Decrypted values must be normalized and agree with their field-specific blind indexes.

The command needs only the subject-key read capability. Unchanged input appends nothing; a concurrent rename invalidates the original exact observation. The combined registration/name observation now also includes effective account deletion; the existing auth-ancestry index covers it. The event contract and projection schema are unchanged. Real-Orisun and HTTP regressions cover ownership, malformed authority, plaintext commitments, initial-name fallback, unchanged retries, concurrent writes and authenticated identity despite unrelated form/view fields.

## Password-change command boundary

`ChangePasswordCommand` contains the authenticated registered-user ID, current/new passwords and metadata. HTTP takes the identity from its authenticated session. The domain command loads strict accepted-user, effective-password and effective-deletion facts in one bounded observation. It validates owner identity and source ordering, rejects deleted users, and checks the current password against the event-authoritative hash. Credential-projection availability, stale email fields and unrelated form identity fields do not control this command.

The existing registered-user and auth-ancestry indexes cover all three criteria. Concurrent password changes or accepted deletions invalidate the same original observation; no projection reload or replacement snapshot is used. Event payloads and lineage remain the ordinary `PasswordChanged` contract.

## Deletion-request command boundary

`RequestAccountDeletionCommand` contains `UserRegisteredID`, `AuthUserID`, the password and metadata. HTTP supplies both IDs from the authenticated session; form IDs and email fields do not select the account. The private context validates canonical identities, binds them to registration, reconstructs the latest authoritative password, validates source ordering and checks existing deletion ancestry. Known requests retain their frozen operation identity. The command has no UI-model or credential-reader dependency and performs no cleanup.

The existing combined authority observation and separate fresh-identity collision observation are preserved, as are their indexes and the request event contract. Real-Orisun tests cover invalid/foreign IDs, incorrect/stale passwords, pending-request identity conflicts, retries and concurrent writes. The HTTP regression uses no credential reader.

## Reset-request recipient discovery

`RequestPasswordResetCommandHandler` takes email intent plus event saving/retrieval and a read-only key port. It first observes `UserRegistered.emailHash`, then observes `AccountDeleted.scopes.userRegisteredId` for the discovered identity. It never obtains a password hash or UI user object from a projection. The existing email and auth-ancestry indexes cover both reads. The HTTP server no longer exposes a credential-reader dependency for these actions. Tests cover normalization, pending/unknown/deleted users, recipient integrity, key absence, generic acknowledgement and deletion between or after the dependent reads.

## Reset-authorization discovery boundary

`AuthorizePasswordResetCommandHandler` needs no user projection or UI user object. Token discovery supplies candidate request/user IDs; event authority binds them and rejects stale, invalid or deleted identities. Registration, request, authorization and completion ordering is validated by full source position. The combined workflow observation includes effective account deletion, and a separate fresh-operation collision observation guards new authorization. The existing auth-ancestry index covers the additional deletion predicate.

HTTP retains only the narrow verification lookup for this flow. Native SQLite tests remove the isolated fixture’s user projection table and prove authorization still works through the real token-discovery store and Orisun events. Domain execution and workflow completion remain separate reactions.

## Deleted-account guards on effective mutations

Name updates and trusted password assignments include `AccountDeleted` for the selected user in their existing bounded authority read. New operations against deleted users return before saving; name updates return before subject-key access, including unchanged-name input. A deletion committed after the read invalidates the same combined observation.

An exact password-operation receipt recorded before deletion remains a successful historical no-op, even after a newer password assignment and account deletion. Its owner/hash identity stays content-bound. Receipts recorded before registration or after deletion are invalid authority. All checks remain in the owning command context; event lineage and workflow reactions are unchanged.

## Registration-start collision observation

Registration start allocates its proposal and planned-user IDs after input validation and before reading authority. Accepted username/email criteria and all three identity collision criteria are known together, so one `GetLatestByCriteria` reads the complete five-criterion inventory. Existing collisions fail before subject-key creation. One unchanged query/returned-position pair guards the proposal append; collisions arriving after that read cause an atomic consistency conflict.

The existing accepted-identity, planned-user and built-in envelope indexes cover the combined query. No event-contract, schema or lineage change is introduced. Real-Orisun tests compare persisted write context with the actual read and inject each collision type before and after that read.

## Registration processing UI

OTP success redirects to the processing page. Read-only workflow status and a narrow credential-readiness query gate the login redirect together. The page and reconnecting stream load current state; projection notifications plus a two-second fallback refresh write only the stage into per-session ViewStore. All SSE patches run through the existing single-writer loop. HTTP never executes registration or completion commands to speed up the UI.

## Password-reset processing UI

Password authorization redirects to a processing page keyed by authorization ID, never the reset token. Read-only status requires matching authorization, effective password change and completion facts with ordered full source positions. Success also requires the credential projection watermark to reach the effect position. The stage-only per-session ViewStore is refreshed every two seconds and on reconnect; the handler's ViewStore loop is the sole SSE writer. Terminal success offers a sign-in link; account deletion makes progress unavailable. No UI route executes password assignment or workflow completion.

## Account-deletion processing UI

Deletion request success clears the session cookie before the first SSE write and redirects to a public processing route keyed by request ID. The route needs no live session because execution removes sessions. Read-only progress binds request, effective deletion and completion to their exact operation, owner and full source ordering. It stays processing through cleanup failure and after effective deletion until workflow completion. Cleanup and key destruction already precede `AccountDeleted`, so no projection-readiness query or key lookup is needed. The stage-only anonymous ViewStore key is refreshed every two seconds and on reconnect; the single-writer SSE loop patches success and closes when complete. Progress reads never execute cleanup or completion commands.

## Auth projection after erasure

Cleanup persists an auth-owned erasure marker and transfers occurrence receipts while removing auth rows under the projection's immediate write transaction. This precedes key destruction, so a backlogged projector need not reach the later `AccountDeleted` event to recognize erased users. Unseen occurrences still pass strict owner decoding; marked identities record only occurrence evidence and never access keys or recreate credentials/tokens. Active identities with missing keys still fail. Retained receipts preserve replay and notification retries after user-row removal. See [auth projection transactions](auth-projection-transactions.md). This slice covers the auth projection, including its composed profile inputs; the separate profile and todo projections remain to be converted.

## Development seed

`internal/developmentseed` composes versioned demo histories through public commands. It reconciles immutable ownership and stable identities before continuing an incomplete prefix, and refuses unrelated edits. Seed-only mode enables projections after composition and waits for every final full-position watermark plus login readiness. Normal startup never seeds. See [development seed](development-seed.md).
