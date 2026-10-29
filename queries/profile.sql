-- name: ProfileUser :one
SELECT user_id,
       coalesce(name, '')             AS name,
       coalesce(username, '')         AS username,
       coalesce(email, '')            AS email,
       coalesce(image, '')            AS image,
       coalesce(bio, '')              AS bio,
       coalesce(header_image_url, '') AS header_image_url
FROM profile_stats
WHERE user_id = @user_id;

-- name: LockProfileProjection :exec
SELECT pg_advisory_xact_lock(hashtextextended('profile-projection', 0));

-- name: FindProcessedProfile :one
SELECT user_id
FROM profile_stats
WHERE event_positions ? @position_key::text
LIMIT 1;

-- name: LoadProfileProjection :one
SELECT user_id,
       name,
       username,
       email,
       bio,
       image,
       header_image_url,
       last_event_commit_position,
       last_event_prepare_position
FROM profile_stats
WHERE user_id = @user_id FOR UPDATE;

-- name: SaveProfileProjection :execrows
INSERT INTO profile_stats AS p(user_id, name, username, email, bio, image, header_image_url, last_event_commit_position,
                               last_event_prepare_position, event_positions)
VALUES (@user_id, @name, @username, @email, @bio, @image, @header_image_url, @last_event_commit_position,
        @last_event_prepare_position, @event_positions::jsonb)
ON CONFLICT(user_id) DO UPDATE SET name=EXCLUDED.name,
                                   username=EXCLUDED.username,
                                   email=EXCLUDED.email,
                                   bio=EXCLUDED.bio,
                                   image=EXCLUDED.image,
                                   header_image_url=EXCLUDED.header_image_url,
                                   last_event_commit_position=EXCLUDED.last_event_commit_position,
                                   last_event_prepare_position=EXCLUDED.last_event_prepare_position,
                                   event_positions=EXCLUDED.event_positions || p.event_positions,
                                   updated_at=now()
WHERE (p.last_event_commit_position, p.last_event_prepare_position) <=
      (EXCLUDED.last_event_commit_position, EXCLUDED.last_event_prepare_position);

-- name: DeleteProfileByRegisteredID :exec
DELETE
FROM profile_stats
WHERE user_id = @user_registered_id;

-- name: ProfileProjectionReached :one
SELECT EXISTS (SELECT 1 FROM profile_stats
 WHERE user_id = @id AND (last_event_commit_position,last_event_prepare_position) >= (@commit_position::bigint,@prepare_position::bigint)) AS reached;
