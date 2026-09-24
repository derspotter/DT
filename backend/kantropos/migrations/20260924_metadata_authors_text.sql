-- Integral Learning Kantropos PostgreSQL schema, not the DT database.
-- Back up metadata and metadata_authors and stop overlapping imports first.
-- Corporate/working-group author entries can legitimately exceed 255 chars.
-- Preserve the complete source value; do not split or truncate it here.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DO $$
DECLARE
    current_type text;
BEGIN
    SELECT format_type(a.atttypid, a.atttypmod) INTO current_type
      FROM pg_attribute a
     WHERE a.attrelid = 'public.metadata_authors'::regclass
       AND a.attname = 'author' AND NOT a.attisdropped;
    IF current_type = 'character varying(255)' THEN
        ALTER TABLE public.metadata_authors ALTER COLUMN author TYPE text;
    ELSIF current_type IS DISTINCT FROM 'text' THEN
        RAISE EXCEPTION 'Unexpected metadata_authors.author type: %', current_type;
    END IF;
END $$;
COMMIT;
