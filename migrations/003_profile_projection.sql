-- Preserve existing state and checkpoints; collect evidence for new deliveries.
ALTER TABLE profile_stats
    ADD COLUMN IF NOT EXISTS event_positions JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(event_positions) = 'object');
