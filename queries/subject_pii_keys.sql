-- name: CreateSubjectPiiKey :exec
INSERT INTO subject_pii_keys (subject_id, encrypted_data_key, encryption_nonce, key_version)
VALUES (@subject_id, @encrypted_data_key, @encryption_nonce, @key_version)
ON CONFLICT (subject_id) DO NOTHING;

-- name: SubjectPiiKey :one
SELECT subject_id, encrypted_data_key, encryption_nonce, key_version
FROM subject_pii_keys
WHERE subject_id = @subject_id;

-- name: DeleteSubjectPiiKey :exec
DELETE FROM subject_pii_keys
WHERE subject_id = @subject_id;
