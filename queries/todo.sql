-- name: ListTodos :many
SELECT todo_id, title, completed, created_at, updated_at
FROM todo_items
WHERE user_id = @user_id
  AND deleted_at IS NULL
ORDER BY created_at DESC, todo_id DESC;


-- name: FindProcessedTodoProjection :one
SELECT todo_id,user_id FROM todo_items
WHERE (last_event_commit_position,last_event_prepare_position) >= (@commit_position,@prepare_position)
  AND json_type(event_positions, '$."' || @position_key || '"') IS NOT NULL
LIMIT 1;

-- name: LoadTodoProjection :one
SELECT todo_id,user_id,title,created_at,updated_at,deleted_at,
       completed_at,completed_commit_position,completed_prepare_position,
       reopened_at,reopened_commit_position,reopened_prepare_position,
       last_event_commit_position,last_event_prepare_position
FROM todo_items WHERE todo_id = @todo_id;

-- name: SaveTodoProjection :execrows
INSERT INTO todo_items(todo_id,user_id,title,created_at,updated_at,deleted_at,
 completed_at,completed_commit_position,completed_prepare_position,
 reopened_at,reopened_commit_position,reopened_prepare_position,
 last_event_commit_position,last_event_prepare_position,event_positions)
VALUES (@todo_id,@user_id,@title,@created_at,@updated_at,@deleted_at,
 @completed_at,@completed_commit_position,@completed_prepare_position,
 @reopened_at,@reopened_commit_position,@reopened_prepare_position,
 @last_event_commit_position,@last_event_prepare_position,@event_positions)
ON CONFLICT (todo_id) DO UPDATE SET
 user_id=excluded.user_id,title=excluded.title,updated_at=excluded.updated_at,deleted_at=excluded.deleted_at,
 completed_at=excluded.completed_at,completed_commit_position=excluded.completed_commit_position,completed_prepare_position=excluded.completed_prepare_position,
 reopened_at=excluded.reopened_at,reopened_commit_position=excluded.reopened_commit_position,reopened_prepare_position=excluded.reopened_prepare_position,
 last_event_commit_position=excluded.last_event_commit_position,last_event_prepare_position=excluded.last_event_prepare_position,
 event_positions=json_patch(todo_items.event_positions, excluded.event_positions)
WHERE (todo_items.last_event_commit_position,todo_items.last_event_prepare_position) < (excluded.last_event_commit_position,excluded.last_event_prepare_position);

-- name: DeleteTodosByUserID :exec
DELETE FROM todo_items
WHERE user_id = @user_id;
