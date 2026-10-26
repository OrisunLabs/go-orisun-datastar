# Orisun Event Store Indexes

Indexes are declared in `internal/eventcatalog/indexes.go` and ensured at startup. Application selectors use `eventstore.EventTypeField` (`eventType`) and `eventId`; the adapter maps these to Orisun’s reserved `__eventType` and `__eventId` for every read, V2 consistency query, subscription and index definition. Payload fields retain their application names.

PII lookup indexes use protected blind-index fields, not plaintext event fields. See `docs/protected-pii-events.md`.

| Index | Fields | Event Types | Query Shape |
| --- | --- | --- | --- |
| `user_registered_email` | `emailHash` | `UserRegistered` | Registration duplicate email check. |
| `user_registered_username` | `usernameHash` | `UserRegistered` | Registration duplicate username check. |
| `user_registered_id` | `userRegisteredId` | `UserRegistered` | User existence/context loads. |
| `email_otp_generated_id` | `emailVerificationOTPGeneratedId` | `EmailVerificationOTPGenerated` | OTP send/validation context lookup. |
| `otp_ancestry` | `scopes.emailVerificationOTPGeneratedId` | `EmailVerificationOTPValidated`, `EmailVerificationOTPSent` | OTP sent/validated idempotency. |
| `password_reset_requested_id` | `passwordResetRequestedId` | `PasswordResetRequested` | Password reset email/send completion context. |
| `reset_ancestry` | `scopes.passwordResetRequestedId` | `PasswordResetEmailSent`, `PasswordResetCompleted` | Password reset idempotency and completion. |
| `auth_user_ancestry` | `scopes.userRegisteredId` | Auth/account descendant events | Per-user stream reconstruction and projections. |
| `profile_owner` | `userId` | Profile image/header/bio events | Cross-domain user reference for latest profile authority and consistency guards. |
| `todo_created` | `todoCreatedId`, `userId` | `TodoCreated` | Creation identity, ownership and collision guard. |
| `todo_ancestry` | `scopes.todoCreatedId`, `userId` | `TodoRenamed`, `TodoCompleted`, `TodoReopened`, `TodoDeleted` | Per-todo authority, ownership and idempotency context. |
| `account_deletion_requested_id` | `accountDeletionRequestedId` | `AccountDeletionRequested` | Account deletion completion lookup. |
| `deletion_ancestry` | `scopes.accountDeletionRequestedId` | `AccountDeleted` | Account deletion idempotency. |
| `login_attempt_recorded_id` | `loginAttemptRecordedId` | `LoginAttemptRecorded` | Login-attempt append subset/idempotency. |
| `login_attempt_identifier_hash` | `attemptedIdentifierHash` | `LoginAttemptRecorded` | Security audit lookup by attempted email/username blind index. |
| `login_attempt_ip_hash` | `ipAddressHash` | `LoginAttemptRecorded` | Security audit lookup by IP blind index. |
| `login_user` | `user.userRegisteredId` | `LoginAttemptRecorded` | Successful login audit lookup by registered user. |

OTP validation batches the latest generated OTP and registered-user criteria in its first read, then checks validation of that discovered OTP in a second read. Both exact queries and positions are checked by one V2 append; the existing generated-by-user and validated-by-OTP indexes support these reads.

When adding an Orisun query or V2 consistency query, add an explicit `eventstore.EventTypeField` criterion and update this table with the matching index.

Todo creation and descendant criteria use different identity paths. Every lifecycle context reads all five event types with these exact paths and checks the returned position during append. The projection subscribes by event type using Orisun’s built-in type indexes. New startup definitions replace the old `todo_scope` definition; no stored events are rewritten and no existing indexes are automatically dropped.

Auth descendants use plural `scopes`. Their ancestry indexes have new semantic names so an old singular-scope index cannot mask installation of the new query shape. Login audit identity uses a tagged `user` object; only its registered variant has `user.userRegisteredId`. Profile events use a separate `profile_owner` index over their top-level `userId`; auth registration and name-change subscriptions continue to use Orisun’s built-in event-type indexes. No old indexes or stored events are automatically dropped or rewritten.

Atomic todo bulk contexts batch the existing five per-todo lifecycle criteria for every caller-selected member, plus a target-event `eventId` receipt criterion for each caller-owned operation ID. `todo_created` / `todo_ancestry` cover lifecycle authority; Orisun’s built-in `__eventType` / `__eventId` selectors cover exact outcome receipts. One combined query/position observation guards the complete append. No additional application index is declared for these built-in envelope lookups.

Todo creation now batches `TodoCreated` envelope-ID collision/retry criteria through `CreateTodos`. `RenameTodos` batches the same five lifecycle predicates as other todo mutations plus `TodoRenamed` envelope-ID receipts, and checks expected titles from the decoded latest facts. Single-item routes use these same batch handlers and index shapes; there are no separate single-item command queries.
