-- name: ProfileUser :one
SELECT user_id,
       CAST(coalesce(name, '') AS TEXT)             AS name,
       CAST(coalesce(username, '') AS TEXT)         AS username,
       CAST(coalesce(email, '') AS TEXT)            AS email,
       CAST(coalesce(image, '') AS TEXT)            AS image,
       CAST(coalesce(bio, '') AS TEXT)              AS bio,
       CAST(coalesce(header_image_url, '') AS TEXT) AS header_image_url
FROM profile_stats
WHERE user_id = @user_id;


-- name: FindProcessedProfile :one
SELECT user_id
FROM profile_stats
WHERE json_type(event_positions, '$."' || @position_key || '"') IS NOT NULL
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
WHERE user_id = @user_id;

-- name: SaveProfileProjection :execrows
INSERT INTO profile_stats AS p(user_id, name, username, email, bio, image, header_image_url, last_event_commit_position,
                               last_event_prepare_position, event_positions)
VALUES (@user_id, @name, @username, @email, @bio, @image, @header_image_url, @last_event_commit_position,
        @last_event_prepare_position, @event_positions)
ON CONFLICT(user_id) DO UPDATE SET name=EXCLUDED.name,
                                   username=EXCLUDED.username,
                                   email=EXCLUDED.email,
                                   bio=EXCLUDED.bio,
                                   image=EXCLUDED.image,
                                   header_image_url=EXCLUDED.header_image_url,
                                   last_event_commit_position=EXCLUDED.last_event_commit_position,
                                   last_event_prepare_position=EXCLUDED.last_event_prepare_position,
                                   event_positions=json_patch(EXCLUDED.event_positions, p.event_positions),
                                   updated_at=CURRENT_TIMESTAMP
WHERE (p.last_event_commit_position, p.last_event_prepare_position) <=
      (EXCLUDED.last_event_commit_position, EXCLUDED.last_event_prepare_position);

-- name: DeleteProfileByRegisteredID :exec
DELETE
FROM profile_stats
WHERE user_id = @user_registered_id;

-- name: ProfileProjectionReached :one
SELECT EXISTS (SELECT 1 FROM profile_stats
 WHERE user_id = @id AND (last_event_commit_position,last_event_prepare_position) >= (@commit_position,@prepare_position)) AS reached;
