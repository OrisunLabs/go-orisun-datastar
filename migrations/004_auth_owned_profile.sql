-- Auth owns the profile facts needed for login/session reads.
ALTER TABLE auth_user
    ADD COLUMN IF NOT EXISTS bio              TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS header_image_url TEXT NOT NULL DEFAULT '';
