-- name: LockAuthProjection :exec
SELECT pg_advisory_xact_lock(734100003);

-- name: AuthOccurrenceOwner :one
SELECT id
FROM auth_user
WHERE event_positions ? @occurrence_key::text
UNION ALL
SELECT user_id AS id
FROM auth_erased_users
WHERE event_positions ? @occurrence_key::text
LIMIT 1;

-- name: LoadAuthProjection :one
SELECT u.id,
       u.user_registered_id,
       u.name,
       coalesce(u.username, '')::text AS username,
       u.email,
       coalesce(u.image, '')::text    AS image,
       u.bio,
       u.header_image_url,
       u.email_verified,
       u.email_verified_at,
       u.last_event_commit_position,
       u.last_event_prepare_position,
       a.id                           AS credential_id,
       a.account_id,
       a.provider_id,
       a.user_id,
       coalesce(a.password, '')::text AS password
FROM auth_user u
         LEFT JOIN auth_account a ON a.id = u.id || ':credential'
WHERE u.id = @id FOR UPDATE OF u;

-- name: LoadAuthVerificationInput :one
SELECT *
FROM auth_verification
WHERE id = @id;

-- name: SaveAuthProjection :exec
INSERT INTO auth_user(id, user_registered_id, name, username, display_username, email, email_verified, image, bio,
                      header_image_url,
                      email_verified_at, event_positions, last_event_commit_position, last_event_prepare_position)
VALUES (@id, @id, @name, @username, @username, @email, @email_verified, @image, @bio, @header_image_url,
        @email_verified_at, @event_positions, @last_event_commit_position, @last_event_prepare_position)
ON CONFLICT(id) DO UPDATE SET name=excluded.name,
                              email_verified=excluded.email_verified,
                              image=excluded.image,
                              bio=excluded.bio,
                              header_image_url=excluded.header_image_url,
                              email_verified_at=excluded.email_verified_at,
                              event_positions=auth_user.event_positions || excluded.event_positions,
                              last_event_commit_position=excluded.last_event_commit_position,
                              last_event_prepare_position=excluded.last_event_prepare_position;

-- name: SaveAuthCredential :exec
INSERT INTO auth_account(id, account_id, provider_id, user_id, password)
VALUES (@id, @account_id, 'credential', @user_id, @password)
ON CONFLICT(id) DO UPDATE SET password=excluded.password;

-- name: SaveAuthVerificationInput :exec
INSERT INTO auth_verification(id, identifier, value, expires_at)
VALUES (@id, @identifier, @value, @expires_at);

-- name: AuthCredentialOwner :one
SELECT user_id
FROM auth_account
WHERE id = @id;

-- name: AuthProjectionErased :one
SELECT EXISTS (SELECT 1 FROM auth_erased_users WHERE user_id = @user_id) AS erased;

-- name: MarkAuthProjectionErased :exec
INSERT INTO auth_erased_users(user_id, event_positions)
VALUES (@user_id, coalesce((SELECT event_positions FROM auth_user WHERE id = @user_id), '{}'::jsonb))
ON CONFLICT(user_id) DO UPDATE SET event_positions = auth_erased_users.event_positions || excluded.event_positions;

-- name: RecordErasedAuthOccurrence :execrows
UPDATE auth_erased_users
SET event_positions = event_positions || @event_positions::jsonb
WHERE user_id = @user_id;
