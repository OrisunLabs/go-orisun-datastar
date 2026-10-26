-- name: ListTodos :many
SELECT todo_id, title, completed, created_at, updated_at
FROM todo_items
WHERE user_id = @user_id
  AND deleted_at IS NULL
ORDER BY created_at DESC, todo_id DESC;

-- name: InsertCreatedTodo :exec
INSERT INTO todo_items (todo_id, user_id, title, completed, completed_at, deleted_at, last_event_commit_position, last_event_prepare_position, created_at, updated_at)
VALUES (@todo_id, @user_id, @title, false, null, null, @last_event_commit_position, @last_event_prepare_position, @created_at, @created_at)
ON CONFLICT (todo_id) DO NOTHING;

-- name: RenameTodo :exec
UPDATE todo_items
SET title = @title,
    last_event_commit_position = @last_event_commit_position,
    last_event_prepare_position = @last_event_prepare_position,
    updated_at = @updated_at
WHERE todo_id = @todo_id;

-- name: CompleteTodo :exec
UPDATE todo_items
SET completed = true,
    completed_at = @completed_at,
    last_event_commit_position = @last_event_commit_position,
    last_event_prepare_position = @last_event_prepare_position,
    updated_at = @completed_at
WHERE todo_id = @todo_id;

-- name: ReopenTodo :exec
UPDATE todo_items
SET completed = false,
    completed_at = null,
    last_event_commit_position = @last_event_commit_position,
    last_event_prepare_position = @last_event_prepare_position,
    updated_at = @updated_at
WHERE todo_id = @todo_id;

-- name: DeleteTodo :exec
UPDATE todo_items
SET deleted_at = @deleted_at,
    last_event_commit_position = @last_event_commit_position,
    last_event_prepare_position = @last_event_prepare_position,
    updated_at = @deleted_at
WHERE todo_id = @todo_id;

-- name: DeleteTodosByUserID :exec
DELETE FROM todo_items
WHERE user_id = @user_id;
