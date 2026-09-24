# Kantropos database compatibility migrations

These migrations target Integral Learning's `kantropos` PostgreSQL database,
not the Corpusbuilder database. They are not applied automatically by DT.

## 2026-09-24: preserve long author entries

The upstream importer inserts vectors before committing each document's
metadata. A 388-character working-group attribution exceeded the
`metadata_authors.author varchar(255)` limit: vectors were already present,
but metadata rolled back. An INSERT-mode retry skips existing vector IDs,
so a retry alone cannot repair that document.

Before applying `20260924_metadata_authors_text.sql`:

1. Verify no import is active and retain a database backup of `metadata` and
   `metadata_authors` with a checked archive listing.
2. Run the SQL using `psql -v ON_ERROR_STOP=1` against the correct database.
   It is transactional, idempotent, and has short lock/statement timeouts.
3. For the saved draft, compare vector document IDs with metadata keys.
   Repair confirmed vector-only documents using the upstream
   `MetadataUpdater.update_metadata_entry`, then commit and verify the full
   author values. Do not delete or regenerate already completed vectors.
4. Resume the same draft with completed OCR, apply, and Markdown stages
   skipped, retaining all preflight checks. Verify vector/metadata coverage
   against the draft after completion, not just an HTTP success status.

The new `text` type survives normal container restarts. Check it again after
upgrading upstream or rebuilding its database schema; this migration does
not change upstream ORM/schema-generation code. Do not roll back by narrowing
to 255 characters after long entries have been stored: that would fail or
require data loss. Keep the pre-migration backup for deliberate recovery.
