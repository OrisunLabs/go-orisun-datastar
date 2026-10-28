# Orisun Event Store Indexes

Indexes are declared in `internal/eventcatalog/indexes.go` and ensured at startup. Application selectors use `eventstore.EventTypeField` (`eventType`) and `eventId`; the adapter maps these to Orisun’s reserved `__eventType` and `__eventId` for every read, V2 consistency query, subscription and index definition. Payload fields retain their application names.

PII lookup indexes use protected blind-index fields, not plaintext event fields. See `docs/protected-pii-events.md`.

| Index | Fields | Event Types | Query Shape |
| --- | --- | --- | --- |
| `registration_started_id` | `userRegistrationStartedId` | `UserRegistrationStarted` | Pending registration authority. |
| `registration_user` | `userId` | `UserRegistrationStarted` | Planned accepted identity collision guard. |
| `registration_ancestry` | `scopes.userRegistrationStartedId` | OTP generated/validated, registration completed/failed | Pending workflow context and completion. |
| `user_registered_email` | `emailHash` | `UserRegistered` | Registration duplicate email check and reset-recipient discovery. |
| `user_registered_username` | `usernameHash` | `UserRegistered` | Registration duplicate username check. |
| `user_registered_id` | `userRegisteredId` | `UserRegistered` | User existence/context loads. |
| `email_otp_generated_id` | `emailVerificationOTPGeneratedId` | `EmailVerificationOTPGenerated` | OTP send/validation context lookup. |
| `otp_ancestry` | `scopes.emailVerificationOTPGeneratedId` | `EmailVerificationOTPValidated`, `EmailVerificationOTPSent` | OTP sent/validated idempotency. |
| `password_reset_authorized_id` | `passwordResetAuthorizedId` | `PasswordResetAuthorized` | Exact frozen reset authorization. |
| `password_reset_operation` | `operationId` | `PasswordResetAuthorized` | Future domain-operation collision guard. |
| `password_reset_completion` | `scopes.passwordResetAuthorizedId`, `scopes.passwordChangedId` | `PasswordResetCompleted` | Exact workflow completion receipt. |
| `password_reset_requested_id` | `passwordResetRequestedId` | `PasswordResetRequested` | Password reset email/send completion context. |
| `reset_ancestry` | `scopes.passwordResetRequestedId` | `PasswordResetEmailSent`, `PasswordResetAuthorized`, `PasswordResetCompleted` | Password reset idempotency and completion. |
| `auth_user_ancestry` | `scopes.userRegisteredId` | Auth/account descendants excluding registration OTPs and reset completion | Per-user contexts and projections; latest effective password-change authority for password-authorized mutations. |
| `profile_owner` | `userId` | Profile image/header/bio events | Cross-domain user reference for latest profile authority and consistency guards. |
| `todo_created` | `todoCreatedId`, `userId` | `TodoCreated` | Creation identity, ownership and collision guard. |
| `todo_ancestry` | `scopes.todoCreatedId`, `userId` | `TodoRenamed`, `TodoCompleted`, `TodoReopened`, `TodoDeleted` | Per-todo authority, ownership and idempotency context. |
| `account_deletion_requested_id` | `accountDeletionRequestedId` | `AccountDeletionRequested` | Exact deletion-request identity/collision lookup. |
| `account_deletion_operation` | `operationId` | `AccountDeletionRequested` | Frozen operation collision guards and completion correlation. |
| `account_deletion_effect_completion` | `scopes.accountDeletedId` | `AccountDeletionCompleted` | Exact domain-effect completion receipt. |
| `deletion_ancestry` | `scopes.accountDeletionRequestedId` | `AccountDeletionCompleted` | Workflow receipt lookup by request. |
| `login_attempt_recorded_id` | `loginAttemptRecordedId` | `LoginAttemptRecorded` | Login-attempt append subset/idempotency. |
| `login_attempt_identifier_hash` | `attemptedIdentifierHash` | `LoginAttemptRecorded` | Security audit lookup by attempted email/username blind index. |
| `login_attempt_ip_hash` | `ipAddressHash` | `LoginAttemptRecorded` | Security audit lookup by IP blind index. |
| `login_user` | `user.userRegisteredId` | `LoginAttemptRecorded` | Successful login audit lookup by registered user. |

OTP validation batches the latest generated OTP and pending-registration criteria in its first read, then checks validation of that discovered OTP in a second read. Both exact queries and positions are checked by one V2 append; the existing registration-ancestry and validated-by-OTP indexes support these reads.

When adding an Orisun query or V2 consistency query, add an explicit `eventstore.EventTypeField` criterion and update this table with the matching index.

Todo creation and descendant criteria use different identity paths. Every lifecycle context reads all five event types with these exact paths and checks the returned position during append. The projection subscribes by event type using Orisun’s built-in type indexes. New startup definitions replace the old `todo_scope` definition; no stored events are rewritten and no existing indexes are automatically dropped.

Auth descendants use plural `scopes`. Their ancestry indexes have new semantic names so an old singular-scope index cannot mask installation of the new query shape. Login audit identity uses a tagged `user` object; only its registered variant has `user.userRegisteredId`. Profile events use a separate `profile_owner` index over their top-level `userId`; auth registration and name-change subscriptions continue to use Orisun’s built-in event-type indexes. No old indexes or stored events are automatically dropped or rewritten.

Atomic todo bulk contexts batch the existing five per-todo lifecycle criteria for every caller-selected member, plus a target-event `eventId` receipt criterion for each caller-owned operation ID. `todo_created` / `todo_ancestry` cover lifecycle authority; Orisun’s built-in `__eventType` / `__eventId` selectors cover exact outcome receipts. One combined query/position observation guards the complete append. No additional application index is declared for these built-in envelope lookups.

Todo creation now batches `TodoCreated` envelope-ID collision/retry criteria through `CreateTodos`. `RenameTodos` batches the same five lifecycle predicates as other todo mutations plus `TodoRenamed` envelope-ID receipts, and checks expected titles from the decoded latest facts. Single-item routes use these same batch handlers and index shapes; there are no separate single-item command queries.

Password changes batch `UserRegistered` by `userRegisteredId` with `PasswordChanged` and `AccountDeleted` by `scopes.userRegisteredId`, guarding the authoritative password and active-account state together. Deletion requests batch the registration/password-change criteria with their existing deletion-request/effective-deletion criteria. Every criterion includes `eventType`. The `user_registered_id` and `auth_user_ancestry` indexes cover these predicates; the unchanged returned combined observation guards the append against concurrent password updates or effective account deletion. No additional index is needed.

Password reset authorization observes its request, accepted user, prior authorization, completion and effective account deletion together. A new authorization takes a separate operation-collision observation. Domain password assignment observes registration, its exact `PasswordChanged` identity, the latest effective password and `AccountDeleted` for the selected user. Reset completion observes authorization by operation ID, the exact effective change and its completion receipt in one query. Registration completion discovers pending registration by planned `userId`, then checks its authorization and exact accepted fact with separate observations. Every observation retains its original query and position.

Account deletion request allocation adds a separate exact request/operation collision observation. Independent domain deletion observes accepted-user authority plus exact operation and latest per-user `AccountDeleted` facts. Completion batches request-by-`operationId`, effective deletion by envelope ID, and completion-by-`scopes.accountDeletedId`. Built-in envelope indexes and the indexes above cover these queries; `AccountDeleted` has no request ancestry.

Password-reset requests observe accepted registration by `emailHash`, then effective deletion by the discovered `scopes.userRegisteredId`. The existing `user_registered_email` and `auth_user_ancestry` indexes cover the two independently retained observations. Unknown or deleted identities append nothing; read-model user/credential tables are not consulted.

Name updates batch accepted registration, latest `UserNameChanged` and effective `AccountDeleted` in one observation. Trusted password assignment adds the same deletion predicate to its registration/receipt/current-password read. The existing `user_registered_id` and `auth_user_ancestry` indexes cover both shapes; no new index is needed.

Registration start batches accepted username/email predicates with `UserRegistrationStarted` envelope-ID, `UserRegistered` envelope-ID and pending `userId` collision predicates in one five-criterion read. Its single append observation retains that exact query and returned position. Existing username/email, registration-user and built-in envelope indexes cover the query.

Registration progress reads the start by envelope ID and validation/completion/failure by registration ancestry in one four-criterion query, then effective deletion for the discovered user. Existing envelope, registration-ancestry and auth-user-ancestry indexes cover these read-only queries. SQL login readiness probes `auth_user` by its primary key and joins the exact credential `auth_account` primary key (`user ID || ':credential'`), checking ownership, verification, provider and a nonempty password. Both lookups are bounded by existing primary-key indexes; no additional index is needed.

Password-reset progress discovers authorization by built-in event type/envelope ID, then batches the discovered password-change envelope ID, completion authorization ancestry and user deletion ancestry. Existing built-in, reset-authorization and auth-user ancestry indexes cover these read-only queries. Credential readiness uses the existing user and exact credential primary keys, comparing the user's full projection watermark to the effect position after those bounded lookups; no additional SQL index is needed.

Account-deletion progress discovers its request by built-in event type/envelope ID, then batches the discovered effective-deletion envelope ID and completion by `scopes.accountDeletedId`. Built-in envelope indexes and `account_deletion_effect_completion` cover the read-only queries. No new index or read-model query is introduced.
