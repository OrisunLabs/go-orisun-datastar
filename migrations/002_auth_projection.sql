-- Idempotent upgrades also apply to databases created before projection evidence.
ALTER TABLE auth_user
    ADD COLUMN IF NOT EXISTS email_validated_id          TEXT,
    ADD COLUMN IF NOT EXISTS email_validation_otp_id     TEXT,
    ADD COLUMN IF NOT EXISTS email_validated_at          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_event_commit_position  BIGINT CHECK (last_event_commit_position >= 0),
    ADD COLUMN IF NOT EXISTS last_event_prepare_position BIGINT CHECK (last_event_prepare_position >= 0),
    ADD COLUMN IF NOT EXISTS event_positions             JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_positions) = 'object');
-- Install the multi-column fact constraint once; existing verification flags remain valid.
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1
                       FROM pg_constraint
                       WHERE conrelid = 'auth_user'::regclass AND conname = 'auth_projection_position_complete') THEN
            ALTER TABLE auth_user
                ADD CONSTRAINT auth_projection_position_complete CHECK (
                    (last_event_commit_position IS NULL) = (last_event_prepare_position IS NULL));
        END IF;
        IF NOT EXISTS (SELECT 1
                       FROM pg_constraint
                       WHERE conrelid = 'auth_user'::regclass
                         AND conname = 'auth_email_validation_complete') THEN
            ALTER TABLE auth_user
                ADD CONSTRAINT auth_email_validation_complete CHECK (
                    (email_validated_id IS NULL) = (email_validation_otp_id IS NULL) AND
                    (email_validated_id IS NULL) = (email_validated_at IS NULL) AND
                    (email_validated_id IS NULL OR email_verified));
        END IF;
    END
$$;
