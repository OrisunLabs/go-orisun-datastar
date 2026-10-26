# Protected PII in Events

Immutable events must not store plaintext user PII.

Canonical PII event fields are encrypted `protectedpii.Value` objects and must include a separate blind-index field when the value is queried. For example, `UserRegistered.email` is protected and `UserRegistered.emailHash` is used for uniqueness lookup.

Required secrets:

- `PII_KEY_ENCRYPTION_SECRET`: stable high-entropy encryption secret.
- `PII_BLIND_INDEX_SECRET`: stable high-entropy HMAC secret for deterministic lookup indexes.

Losing or changing these secrets breaks decryption and blind-index lookup for existing events. Development has fallback secrets, but production deployments must set both values.

Each planned or registered user has a subject data key in `subject_pii_keys`. The key is encrypted with `PII_KEY_ENCRYPTION_SECRET`; event PII is encrypted with the subject data key. Destroying the subject key makes historical PII event payloads undecryptable while preserving immutable event history.

Account deletion follows this event flow:

1. `AccountDeletionRequested` records the user intent after password verification.
2. `account_deletion_event_handler` deletes PostgreSQL read-model/auth data, removes known local profile media objects, and destroys the subject key.
3. `AccountDeleted` records completion.

Pending registration encrypts its frozen details using the planned accepted user ID’s key. Accepted registration carries its own complete encrypted details; consumers never load the pending workflow to decrypt or project the accepted fact.
