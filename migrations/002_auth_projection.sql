-- Auth stores verification from the accepted registration fact.
ALTER TABLE auth_user
    ADD COLUMN IF NOT EXISTS email_verified_at           TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_event_commit_position  BIGINT CHECK (last_event_commit_position >= 0),
    ADD COLUMN IF NOT EXISTS last_event_prepare_position BIGINT CHECK (last_event_prepare_position >= 0),
    ADD COLUMN IF NOT EXISTS event_positions             JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_positions) = 'object');
-- Reapplication preserves the source-position and verification constraints.
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1
                       FROM pg_constraint
                       WHERE conrelid = 'auth_user'::regclass
                         AND conname = 'auth_projection_position_complete') THEN
            ALTER TABLE auth_user
                ADD CONSTRAINT auth_projection_position_complete CHECK (
                    (last_event_commit_position IS NULL) = (last_event_prepare_position IS NULL));
        END IF;
        IF NOT EXISTS (SELECT 1
                       FROM pg_constraint
                       WHERE conrelid = 'auth_user'::regclass AND conname = 'auth_email_validation_complete') THEN
            ALTER TABLE auth_user
                ADD CONSTRAINT auth_email_validation_complete CHECK (email_verified = (email_verified_at IS NOT NULL));
        END IF;
    END
$$;
