CREATE TABLE IF NOT EXISTS projector_checkpoint
(
    id               TEXT PRIMARY KEY,
    name             TEXT UNIQUE NOT NULL,
    commit_position  BIGINT      NOT NULL,
    prepare_position BIGINT      NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS auth_user
(
    id                 TEXT PRIMARY KEY,
    name               TEXT        NOT NULL,
    email              TEXT        NOT NULL UNIQUE,
    email_verified     BOOLEAN     NOT NULL DEFAULT false,
    image              TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    username           TEXT UNIQUE,
    display_username   TEXT,
    role               TEXT,
    banned             BOOLEAN              DEFAULT false,
    ban_reason         TEXT,
    ban_expires        TIMESTAMPTZ,
    user_registered_id TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS auth_user_email_idx ON auth_user (email);
CREATE INDEX IF NOT EXISTS auth_user_username_idx ON auth_user (username);
CREATE INDEX IF NOT EXISTS auth_user_user_registered_id_idx ON auth_user (user_registered_id);

CREATE TABLE IF NOT EXISTS auth_account
(
    id                       TEXT PRIMARY KEY,
    account_id               TEXT        NOT NULL,
    provider_id              TEXT        NOT NULL,
    user_id                  TEXT        NOT NULL REFERENCES auth_user (id) ON DELETE CASCADE,
    access_token             TEXT,
    refresh_token            TEXT,
    id_token                 TEXT,
    access_token_expires_at  TIMESTAMPTZ,
    refresh_token_expires_at TIMESTAMPTZ,
    scope                    TEXT,
    password                 TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS account_userId_idx ON auth_account (user_id);

CREATE TABLE IF NOT EXISTS auth_session
(
    id              TEXT PRIMARY KEY,
    expires_at      TIMESTAMPTZ NOT NULL,
    token           TEXT        NOT NULL UNIQUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ip_address      TEXT,
    user_agent      TEXT,
    user_id         TEXT        NOT NULL REFERENCES auth_user (id) ON DELETE CASCADE,
    impersonated_by TEXT
);

CREATE INDEX IF NOT EXISTS session_userId_idx ON auth_session (user_id);

CREATE TABLE IF NOT EXISTS auth_verification
(
    id         TEXT PRIMARY KEY,
    identifier TEXT        NOT NULL,
    value      TEXT        NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS verification_identifier_idx ON auth_verification (identifier);

CREATE TABLE IF NOT EXISTS subject_pii_keys
(
    subject_id         TEXT PRIMARY KEY,
    encrypted_data_key TEXT        NOT NULL,
    encryption_nonce   TEXT        NOT NULL,
    key_version        TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS profile_stats
(
    user_id                     TEXT PRIMARY KEY,
    name                        TEXT,
    username                    TEXT,
    email                       TEXT,
    image                       TEXT,
    bio                         TEXT,
    header_image_url            TEXT,
    last_event_commit_position  BIGINT      NOT NULL,
    last_event_prepare_position BIGINT      NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS profile_stats_user_id_idx ON profile_stats (user_id);
CREATE INDEX IF NOT EXISTS profile_stats_username_idx ON profile_stats (username);

CREATE TABLE IF NOT EXISTS todo_items (
 todo_id TEXT PRIMARY KEY,
 user_id TEXT NOT NULL,
 title TEXT NOT NULL,
 completed_at TIMESTAMPTZ,
 completed_commit_position BIGINT,
 completed_prepare_position BIGINT,
 reopened_at TIMESTAMPTZ,
 reopened_commit_position BIGINT,
 reopened_prepare_position BIGINT,
 completed BOOLEAN GENERATED ALWAYS AS (
  completed_commit_position IS NOT NULL AND
  (reopened_commit_position IS NULL OR
   (completed_commit_position,completed_prepare_position) > (reopened_commit_position,reopened_prepare_position))
 ) STORED NOT NULL,
 deleted_at TIMESTAMPTZ,
 last_event_commit_position BIGINT NOT NULL CHECK(last_event_commit_position >= 0),
 last_event_prepare_position BIGINT NOT NULL CHECK(last_event_prepare_position >= 0),
 event_positions JSONB NOT NULL DEFAULT '{}'::jsonb CHECK(jsonb_typeof(event_positions)='object'),
 created_at TIMESTAMPTZ NOT NULL,
 updated_at TIMESTAMPTZ NOT NULL,
 CHECK ((completed_at IS NULL)=(completed_commit_position IS NULL) AND (completed_at IS NULL)=(completed_prepare_position IS NULL)),
 CHECK ((reopened_at IS NULL)=(reopened_commit_position IS NULL) AND (reopened_at IS NULL)=(reopened_prepare_position IS NULL)),
 CHECK (reopened_at IS NULL OR completed_at IS NOT NULL),
 CHECK (completed_commit_position IS NULL OR (completed_commit_position>=0 AND completed_prepare_position>=0 AND (completed_commit_position,completed_prepare_position)<=(last_event_commit_position,last_event_prepare_position))),
 CHECK (reopened_commit_position IS NULL OR (reopened_commit_position>=0 AND reopened_prepare_position>=0 AND (reopened_commit_position,reopened_prepare_position)<=(last_event_commit_position,last_event_prepare_position)))
);
CREATE INDEX IF NOT EXISTS todo_items_user_active_created_idx ON todo_items(user_id,created_at,todo_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS todo_items_user_completed_idx ON todo_items(user_id,completed);
CREATE INDEX IF NOT EXISTS todo_items_projection_position_idx ON todo_items(last_event_commit_position,last_event_prepare_position);
