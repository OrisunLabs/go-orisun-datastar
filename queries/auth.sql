-- name: CreateAuthUser :exec
INSERT INTO auth_user (id, name, email, email_verified, username, display_username, user_registered_id)
VALUES (@id, @name, @email, false, @username, @username, @user_registered_id);

-- name: CreateAuthAccount :exec
INSERT INTO auth_account (id, account_id, provider_id, user_id, password)
VALUES (@id, @account_id, 'credential', @user_id, @password);

-- name: CreateAuthSession :exec
INSERT INTO auth_session (id, token, user_id, expires_at)
VALUES (@id, @token, @user_id, @expires_at);

-- name: DeleteAuthSessionByToken :exec
DELETE
FROM auth_session
WHERE token = @token;

-- name: UserBySessionToken :one
SELECT u.id,
       u.user_registered_id,
       u.name,
       coalesce(u.username, '')         AS username,
       u.email,
       u.email_verified,
       coalesce(u.image, '')            AS image,
       coalesce(u.bio, '')              AS bio,
       coalesce(u.header_image_url, '') AS header_image_url
FROM auth_session s
         JOIN auth_user u ON u.id = s.user_id
WHERE s.token = @token
  AND s.expires_at > now();

-- name: UserByRegisteredID :one
SELECT u.id,
       u.user_registered_id,
       u.name,
       coalesce(u.username, '')         AS username,
       u.email,
       u.email_verified,
       coalesce(u.image, '')            AS image,
       coalesce(u.bio, '')              AS bio,
       coalesce(u.header_image_url, '') AS header_image_url
FROM auth_user u
WHERE u.user_registered_id = @user_registered_id;

-- name: UserByIDOrRegisteredID :one
SELECT u.id,
       u.user_registered_id,
       u.name,
       coalesce(u.username, '')         AS username,
       u.email,
       u.email_verified,
       coalesce(u.image, '')            AS image,
       coalesce(u.bio, '')              AS bio,
       coalesce(u.header_image_url, '') AS header_image_url
FROM auth_user u
WHERE u.id = @id
   OR u.user_registered_id = @id;

-- name: CreateAuthVerification :exec
INSERT INTO auth_verification (id, identifier, value, expires_at)
VALUES (@id, @identifier, @value, @expires_at);

-- name: MarkAuthUserEmailVerified :exec
UPDATE auth_user
SET email_verified = true,
    updated_at     = now()
WHERE user_registered_id = @user_registered_id;

-- name: PasswordResetVerificationByToken :one
SELECT id, identifier
FROM auth_verification
WHERE identifier LIKE 'password-reset:%'
  AND value = @value
  AND expires_at > now()
ORDER BY created_at DESC
LIMIT 1;

-- name: UpdateAuthUserName :exec
UPDATE auth_user
SET name       = @name,
    updated_at = now()
WHERE id = @id;

-- name: UpdateAuthUserNameByRegisteredID :exec
UPDATE auth_user
SET name       = @name,
    updated_at = now()
WHERE user_registered_id = @user_registered_id;

-- name: UpdateAuthAccountPassword :exec
UPDATE auth_account
SET password   = @password,
    updated_at = now()
WHERE user_id = @user_id
  AND provider_id = 'credential';

-- name: UpdateAuthAccountPasswordByRegisteredID :exec
UPDATE auth_account
SET password   = @password,
    updated_at = now()
WHERE user_id = (SELECT id
                 FROM auth_user
                 WHERE user_registered_id = @user_registered_id)
  AND provider_id = 'credential';

-- name: UserByEmailWithPassword :one
SELECT u.id,
       u.user_registered_id,
       u.name,
       coalesce(u.username, '')         AS username,
       u.email,
       u.email_verified,
       coalesce(u.image, '')            AS image,
       coalesce(u.bio, '')              AS bio,
       coalesce(u.header_image_url, '') AS header_image_url,
       a.password
FROM auth_user u
         JOIN auth_account a ON a.user_id = u.id AND a.provider_id = 'credential'
WHERE u.email = @email;

-- name: DeleteAuthVerificationsByRegisteredID :exec
DELETE
FROM auth_verification
WHERE identifier IN ('email:' || @user_registered_id, 'password-reset:' || @user_registered_id);

-- name: DeleteAuthSessionsByRegisteredID :exec
DELETE
FROM auth_session
WHERE user_id IN (SELECT id
                  FROM auth_user
                  WHERE user_registered_id = @user_registered_id);

-- name: DeleteAuthAccountsByRegisteredID :exec
DELETE
FROM auth_account
WHERE user_id IN (SELECT id
                  FROM auth_user
                  WHERE user_registered_id = @user_registered_id);

-- name: DeleteAuthUserByRegisteredID :exec
DELETE
FROM auth_user
WHERE user_registered_id = @user_registered_id;
