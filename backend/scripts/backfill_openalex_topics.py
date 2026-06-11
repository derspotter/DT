"""Backfill OpenAlex topic/field/domain classification for stored works.

The original enrichment never requested OpenAlex's topical fields (its
`select` clause omitted `primary_topic`/`topics`), so works carry OpenAlex
IDs and DOIs but no academic-field classification. This script fills that gap
by batch-looking-up works through the OpenAlex API and storing the
topic -> subfield -> field -> domain hierarchy in dedicated columns.

Lookups use `filter=openalex_id:...|...` (and a DOI fallback), which the
shared OpenAlex client treats as non-billable, so this does not consume the
billable daily credit pool.

Usage:
    python backend/scripts/backfill_openalex_topics.py --db-path <path> [--limit N] [--no-doi-fallback]
"""

import argparse
import json
import re
import sqlite3
import sys
import time
from pathlib import Path

from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)
ROOT = Path(__file__).resolve().parents[2]
DL_LIT_PROJECT = ROOT / "dl_lit_project"
if str(DL_LIT_PROJECT) not in sys.path:
    sys.path.insert(0, str(DL_LIT_PROJECT))

from dl_lit.utils import (  # noqa: E402
    OpenAlexRateLimitExceeded,
    get_openalex_mailto,
    openalex_request_json,
)

NEW_COLUMNS = {
    "primary_topic": "TEXT",
    "primary_subfield": "TEXT",
    "primary_field": "TEXT",
    "primary_domain": "TEXT",
    "topics_json": "TEXT",
    "topics_fetched": "INTEGER",
}
SELECT_FIELDS = "id,doi,display_name,primary_topic,topics"
OPENALEX_OR_LIMIT = 50


def log(message):
    print(message, file=sys.stderr, flush=True)


def normalize_openalex_id(value):
    if not value:
        return None
    match = re.search(r"(W\d+)", str(value), re.IGNORECASE)
    return match.group(1).upper() if match else None


def normalize_doi(value):
    if not value:
        return None
    text = str(value).strip()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    return text.lower() or None


def ensure_columns(conn):
    existing = {row[1] for row in conn.execute("PRAGMA table_info(works)")}
    added = []
    for column, column_type in NEW_COLUMNS.items():
        if column not in existing:
            conn.execute(f"ALTER TABLE works ADD COLUMN {column} {column_type}")
            added.append(column)
    if added:
        conn.commit()
        log(f"Added columns: {', '.join(added)}")


def chunk(items, size):
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def topic_fields(work):
    """Extract (topic, subfield, field, domain, topics_json) from a work record."""
    primary = work.get("primary_topic") or {}
    if not isinstance(primary, dict):
        primary = {}

    def name(node):
        return node.get("display_name") if isinstance(node, dict) else None

    topics = work.get("topics") if isinstance(work.get("topics"), list) else []
    compact_topics = [
        {
            "display_name": topic.get("display_name"),
            "field": name(topic.get("field")),
            "domain": name(topic.get("domain")),
            "score": topic.get("score"),
        }
        for topic in topics
        if isinstance(topic, dict)
    ]
    return (
        primary.get("display_name"),
        name(primary.get("subfield")),
        name(primary.get("field")),
        name(primary.get("domain")),
        json.dumps(compact_topics, ensure_ascii=False) if compact_topics else None,
    )


def write_results(conn, updates):
    """updates: list of (work_id, topic, subfield, field, domain, topics_json)."""
    conn.executemany(
        """
        UPDATE works
        SET primary_topic = ?, primary_subfield = ?, primary_field = ?,
            primary_domain = ?, topics_json = ?, topics_fetched = 1
        WHERE id = ?
        """,
        [(topic, subfield, field, domain, topics_json, work_id)
         for (work_id, topic, subfield, field, domain, topics_json) in updates],
    )
    conn.commit()


def mark_fetched(conn, work_ids):
    """Mark resolved-but-topicless works so re-runs skip them."""
    conn.executemany(
        "UPDATE works SET topics_fetched = 1 WHERE id = ? AND topics_fetched IS NULL",
        [(work_id,) for work_id in work_ids],
    )
    conn.commit()


def run_pass(conn, rows, key, build_filter, mailto, batch_size, label):
    """Generic batched backfill. `key` maps a result work to its lookup token."""
    processed = 0
    matched = 0
    fielded = 0
    total = len(rows)
    for batch in chunk(rows, batch_size):
        token_to_work_id = {token: work_id for work_id, token in batch}
        try:
            data = openalex_request_json(
                endpoint="works",
                params={
                    "filter": build_filter([token for _wid, token in batch]),
                    "per-page": batch_size,
                    "select": SELECT_FIELDS,
                },
                mailto=mailto,
            )
        except OpenAlexRateLimitExceeded as exc:
            log(f"[{label}] rate limited, stopping early: {exc}")
            break
        except Exception as exc:  # network / HTTP errors: skip batch, keep going
            log(f"[{label}] batch error ({exc}); skipping {len(batch)} works")
            processed += len(batch)
            continue

        updates = []
        seen_work_ids = []
        for work in data.get("results", []):
            token = key(work)
            work_id = token_to_work_id.get(token)
            if work_id is None:
                continue
            seen_work_ids.append(work_id)
            topic, subfield, field, domain, topics_json = topic_fields(work)
            if field:
                fielded += 1
            updates.append((work_id, topic, subfield, field, domain, topics_json))
        if updates:
            write_results(conn, updates)
            matched += len(updates)
        # Resolved IDs with no usable topic still get flagged so we don't refetch.
        if seen_work_ids:
            mark_fetched(conn, seen_work_ids)
        processed += len(batch)
        log(f"[{label}] {processed}/{total} processed, {matched} resolved, {fielded} with a field")
    return processed, matched, fielded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--batch-size", type=int, default=OPENALEX_OR_LIMIT)
    parser.add_argument("--limit", type=int, default=0, help="cap works processed (0 = all)")
    parser.add_argument("--scope", choices=["matched", "all"], default="matched")
    parser.add_argument("--no-doi-fallback", action="store_true")
    args = parser.parse_args()

    batch_size = max(1, min(OPENALEX_OR_LIMIT, args.batch_size))
    mailto = get_openalex_mailto()

    conn = sqlite3.connect(args.db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 60000")
    ensure_columns(conn)

    scope_clause = (
        "metadata_status = 'matched'" if args.scope == "matched" else "1 = 1"
    )
    limit_clause = f" LIMIT {int(args.limit)}" if args.limit else ""

    # Pass 1: works with an OpenAlex ID.
    id_rows = []
    for row in conn.execute(
        f"""
        SELECT id, openalex_id FROM works
        WHERE {scope_clause}
          AND openalex_id IS NOT NULL AND openalex_id <> ''
          AND (topics_fetched IS NULL OR topics_fetched = 0)
        ORDER BY id{limit_clause}
        """
    ):
        token = normalize_openalex_id(row["openalex_id"])
        if token:
            id_rows.append((row["id"], token))
    log(f"Pass 1 (by openalex_id): {len(id_rows)} works")
    run_pass(
        conn,
        id_rows,
        key=lambda work: normalize_openalex_id(work.get("id")),
        build_filter=lambda tokens: "openalex_id:" + "|".join(tokens),
        mailto=mailto,
        batch_size=batch_size,
        label="id",
    )

    # Pass 2: matched works without an OpenAlex ID but with a DOI.
    if not args.no_doi_fallback:
        doi_rows = []
        for row in conn.execute(
            f"""
            SELECT id, doi FROM works
            WHERE {scope_clause}
              AND (openalex_id IS NULL OR openalex_id = '')
              AND doi IS NOT NULL AND doi <> ''
              AND (topics_fetched IS NULL OR topics_fetched = 0)
            ORDER BY id{limit_clause}
            """
        ):
            token = normalize_doi(row["doi"])
            if token:
                doi_rows.append((row["id"], token))
        log(f"Pass 2 (by doi): {len(doi_rows)} works")
        run_pass(
            conn,
            doi_rows,
            key=lambda work: normalize_doi(work.get("doi")),
            build_filter=lambda tokens: "doi:" + "|".join(tokens),
            mailto=mailto,
            batch_size=batch_size,
            label="doi",
        )

    # Final coverage report.
    total = conn.execute(
        f"SELECT COUNT(*) FROM works WHERE {scope_clause} AND title IS NOT NULL AND title <> ''"
    ).fetchone()[0]
    with_field = conn.execute(
        f"SELECT COUNT(*) FROM works WHERE {scope_clause} AND primary_field IS NOT NULL AND primary_field <> ''"
    ).fetchone()[0]
    log(f"Done. {with_field}/{total} matched+titled works now have a field "
        f"({100 * with_field / total:.1f}%)." if total else "Done.")
    conn.close()


if __name__ == "__main__":
    main()
