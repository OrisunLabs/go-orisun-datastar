-- Retain only routing identity and occurrence evidence after auth PII erasure.
-- This table deliberately has no FK to the user row that cleanup removes.
CREATE TABLE IF NOT EXISTS auth_erased_users
(
    user_id         TEXT PRIMARY KEY,
    event_positions JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_positions) = 'object')
);
CREATE INDEX IF NOT EXISTS auth_erased_users_occurrences_idx ON auth_erased_users USING gin (event_positions);
-- Both branches of auth occurrence lookup use the same JSON existence predicate.
CREATE INDEX IF NOT EXISTS auth_user_occurrences_idx ON auth_user USING gin (event_positions);
