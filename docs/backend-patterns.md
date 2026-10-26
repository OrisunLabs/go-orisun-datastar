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

Command consistency is implemented across existing handlers. The following architectural conversions remain:

- Todo, auth and profile event contracts are converted as described below. Any future event format change requires a coordinated update to constructors, decoders, consumers, queries, indexes and fixtures, with an explicit decision about existing stored history. Do not add mixed decoders or silently rewrite events.
- Command slices now own their criteria and context loaders across auth, todo and profile. Keep authority helpers private to the behavior; do not introduce configurable context frameworks.
- Todo bulk actions discover candidates from the projection and invoke individual authoritative commands. They are not atomic batch commands; a complete batch requires an explicit caller-owned inventory and independent per-item authority guards. Do not imply projection selection is authoritative.
- Keep future event consumers behind strict per-event decoding, including nested protected PII. Plaintext validation requires the owning key and must complete before projection writes or other effects.
- Existing read models use their current sqlc projection operations and full-position guards. They have not been converted to a typed source-ordered projection fold with a transaction-bound repository contract. Convert each owner as one unit; publish invalidations only after commit.
- Existing NATS KV session view-state remains the rendering path. Its CQRS split already follows the CQRS principle: commands acknowledge acceptance, projection notifications invalidate, and SSE reads and patches server-rendered fragments.

Database resets, reseeding and historical event conversions require explicit authorization.

## Todo event-contract conversion

All five todo events have typed payloads, checked constructors and strict decoders in `internal/features/todo/events.go`. `TodoCreated.todoCreatedId` equals the envelope event ID. Every todo event carries a required top-level `userId` as an immutable cross-domain entity reference. Creation is the todo root and omits `scopes`; subsequent facts carry only `scopes.todoCreatedId`. Sharing a physical event boundary does not turn an auth event into todo ancestry. The HTTP adapter supplies the authenticated user’s stable identity as `UserID`; todo commands do not depend on registration-event names. Todo projections store ownership in `todo_items.user_id`. The public command/result/view `TodoID` remains the entity identifier used by routes and read models; its value is the creation event ID.

Decoders require exact fields and types, canonical nonzero UUIDs, matching payload/envelope identity, normalized nonempty titles of at most 160 bytes and canonical nonzero UTC instants. Constructors normalize input instants to UTC and return errors for invalid facts. Every retrieved todo authority is decoded before command decisions; every projection event is decoded before writing or publishing. Invalid timestamps are never replaced by the current time. Queries and indexes distinguish creation identity from descendant ancestry.

The real-Orisun lifecycle test exercises create, rename, complete, reopen and delete through public commands. Corrupt-history tests cover all five contracts, including unknown/missing/null fields, invalid timestamps, legacy scopes and envelope mismatch. Malformed events selected by command queries fail before append; the boundary-wide projection also rejects malformed routing fields that cannot match a targeted command query. Existing ownership, retry and concurrent deletion tests remain active.

This is a breaking todo event format and fresh-install projection schema change. Existing installations also require an explicit migration or recreation decision for the former `todo_items.user_registered_id` column. Existing singular-scope todo events are not upcast or rewritten: they fail strict decoding on replay. Existing projected todos are not automatically converted. Deployments with old todo history must explicitly choose a historical conversion strategy or authorize recreation of disposable development data before adopting this format. No database reset or reseed has been performed.

The todo conversion leaves command query ownership, bulk-action atomicity and projection transaction architecture as separate work. Auth contracts are converted in the next section; profile contracts are converted below.

## Auth event-contract conversion

All twelve auth event types have checked constructors, typed strict decoders and a dispatch validator in `internal/auth/events.go`. Registration is a root and omits scopes. Auth descendants use plural `scopes` containing exact auth event backlinks. `AccountDeleted` keeps its deletion-request backlink only in scopes. Login auditing is a root with an explicit `user.kind` of `anonymous` or `registered`; only the registered variant contains `userRegisteredId`, and successful attempts require that variant.

Validation checks exact required fields, nested encrypted-value structure, canonical UUIDs and event-specific envelope identity, canonical UTC instants, six-digit OTPs, bcrypt password-hash structure, blind-index structure and duplicated blind-index agreement. Constructors propagate encryption failures. Decoding does not require decrypted subject keys, so validation remains possible after key erasure. Consumers that need plaintext return decryption/authentication errors before applying effects.

Commands validate selected authoritative occurrences before interpreting them. Reactions validate source events before invoking commands. Auth projections decode before key access or writes, require validated OTP ancestry instead of falling back to a history scan, and decrypt password-reset tokens before creating verification rows. Reset-token plaintext is checked against its sensitive blind index. Profile consumers of registration/name-change facts use auth’s updated decoders with profile-owned payloads covered by their separate conversion below. The registration reaction passes the stable auth identity rather than interpreting encrypted PII as strings.

Real-Orisun tests retain OTP consistency/concurrent-replacement checks and exercise registration and password-reset requests through public commands. Additional contract tests reject unknown, missing, null and legacy fields across every auth type before projection effects. The reset projection regression verifies the actual decrypted token and rejects tampered ciphertext before another write.

This is a breaking auth event-format change. Existing singular-scope auth history, empty registration scopes and the previous optional login user field are not accepted by the new decoders. Existing deployments need an explicit conversion or disposable-development recreation decision. No reset, reseed, event rewriting or index removal is automatic. Projection transaction architecture and command query ownership remain separate slices.

## Profile event-contract conversion

Bio, avatar and header-image events are independent profile facts with event-specific IDs matching their envelopes, a top-level cross-domain `userId` and no auth-event scopes. Profile command intents accept `UserID`, translated from the authenticated identity at the HTTP edge. The profile read-model schema already uses `user_id`; its ownership index in Orisun is now `profile_owner` over `userId`. No source-registration event name appears in profile payloads.

Checked constructors and strict decoders enforce exact required fields, canonical nonzero UUIDs, canonical UTC instants, structured encrypted bio values and matching blind indexes. Image URLs must be root-relative paths or absolute HTTP(S) URLs, with no protocol-relative form, credentials, fragments, backslashes or whitespace/control characters. Empty bio values remain valid for clearing; decryption validates normalized plaintext, the 280-byte limit and blind-index agreement. PII authentication failures return errors rather than panicking or applying empty defaults.

Commands decode selected profile authority before decisions. Image commands validate the resulting event/URL before uploading bytes. The read-model handler and avatar-to-auth bridge decode profile facts before effects; minimal `userId` notifications follow successful persistence. Profile consumes only a read capability for subject PII keys. Auth registration/name-change subscriptions retain their own typed source decoders and translate those facts at the consumer edge.

Real-Orisun command tests cover bio round trips, unchanged retries, clearing, ciphertext tampering, concurrent bio updates, avatar/header ownership references and rejection of an invalid storage URL before upload. Malformed-contract tests cover all three event types and reject old scopes, unknown/missing/null fields and invalid owners before projection or bridge calls.

This is a breaking profile event format change. Old singular-scope profile history requires an explicit conversion or disposable-development recreation decision; no reset, reseed or historical rewrite has been performed. Command context/query ownership is completed in the next section. Bulk-action atomicity, projection transaction architecture and SSE lifecycle verification remain separate work.

## Command context and query ownership

Each auth, todo and profile behavior owns its private contexts, context loaders and query builders in its command-handler file. Shared query bundles have been removed. Account-deletion request and completion are separate behavior files; shared consumer ports remain in `ports.go`. Email command entrypoints delegate all authoritative reads, validation, decryption and context interpretation to their private loaders. Login auditing owns a save-only context containing the validated event and its collision observation.

Each command retains its existing criteria and batches of independent observations. Loaders preserve the exact position returned by each read rather than recomputing positions from fetched events. Each mutating behavior has one terminal `SaveCommandEventsV2` site. Pure title validation, MIME-extension mapping, event contracts, audit metadata and external-effect interfaces remain shareable. Exported commands can still be composed by orchestration wrappers; the existing todo bulk commands remain projection-selected per-item orchestration until their separate atomicity conversion.

`TestCommandAuthorityHelpersStayWithinOwningSlice` follows local helper calls and checks context ownership, rejects shared authority/query helpers and history scans, requires public handlers to delegate latest-event reads to loaders, and enforces one behavior per file and no multiple terminal save sites. Tests inspect persisted facts with independent queries instead of calling production command query builders.

Real-Orisun email tests verify recorded-send redelivery, failed-send retries, decoded recipients/content and persisted consistency contexts. Existing OTP replacement, todo deletion and profile bio race tests continue to exercise the original guards. This conversion changes code ownership without changing event contracts, query shapes, database schema or public command signatures. No reset or reseed was performed. Bulk-action atomicity is the next slice; projection transactions and SSE lifecycle remain outstanding.
