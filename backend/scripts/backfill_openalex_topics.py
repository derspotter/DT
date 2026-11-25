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
import sqlite3
import sys
from pathlib import Path

from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)
ROOT = Path(__file__).resolve().parents[2]
DL_LIT_PROJECT = ROOT / "dl_lit_project"
if str(DL_LIT_PROJECT) not in sys.path:
    sys.path.insert(0, str(DL_LIT_PROJECT))

from dl_lit.identifiers import (  # noqa: E402
    _DOI_IN_TEXT_RE,
    _DOI_SCHEME_PREFIX_RE,
    _DOI_URL_PREFIX_RE,
    normalize_doi,
    normalize_openalex_id,
)
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


def bare_doi(raw) -> str | None:
    """A whole bare DOI in lowercase, or ``None``.

    Strips whitespace, rejects non-ASCII input (before any case change, so a
    Kelvin sign never folds into ``k``), lowercases, drops only a ``doi.org``
    URL or ``doi:`` prefix, and keeps the token only when the entire rest is a
    DOI under the DB rule's pattern. Unlike ``normalize_doi`` it never takes a
    key out of a longer text: ``10.1000/ABC+123`` is ``None``, not ``10.1000/abc``.
    Trailing punctuation stays part of the DOI, as it does in the DB key.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or not text.isascii():
        return None
    text = text.lower()
    text = _DOI_URL_PREFIX_RE.sub("", text)
    text = _DOI_SCHEME_PREFIX_RE.sub("", text)
    token = text.strip()
    return token if _DOI_IN_TEXT_RE.fullmatch(token) else None


def doi_filter(tokens: list[str]) -> str:
    return "doi:" + "|".join(token.lower() for token in tokens)


def doi_rows(conn, *, scope_clause: str, limit_clause: str) -> tuple[list[tuple[int, str, str]], int]:
    """Pass 2's candidates as ``(work_id, DB key, raw doi)``, plus how many were not a whole DOI.

    The raw ``doi`` is the column's value exactly as read; the write guard compares against it.
    """
    rows = []
    skipped = 0
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
        bare = bare_doi(row[1])
        if bare:
            rows.append((row[0], bare.upper(), row[1]))  # bare.upper() == normalize_doi(bare), the DB key
        else:
            skipped += 1
    return rows, skipped


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


# What each pass selected a row for, repeated in every write (the scope comes on top), so a row
# the enrich worker or a merge changed while its request was held is left for the next run.
WRITE_GUARDS: dict[str, str] = {
    "openalex": "openalex_id = ? AND (topics_fetched IS NULL OR topics_fetched = 0)",
    "doi": "doi = ? AND (openalex_id IS NULL OR openalex_id = '') AND (topics_fetched IS NULL OR topics_fetched = 0)",
}


def write_results(conn, updates, guard: str, scope_clause: str) -> list[int]:
    """Write topics for rows still in the state the pass read; return the ids written.

    updates: list of (work_id, raw, topic, subfield, field, domain, topics_json), where ``raw``
    is the identifier the row was read with. ``guard`` names a ``WRITE_GUARDS`` entry and
    ``scope_clause`` is ``main``'s fixed scope; neither comes from free input. Every resolved
    row gets ``topics_fetched = 1`` here, with or without a usable topic.
    """
    sql = f"""
        UPDATE works
        SET primary_topic = ?, primary_subfield = ?, primary_field = ?,
            primary_domain = ?, topics_json = ?, topics_fetched = 1
        WHERE id = ? AND ({scope_clause}) AND {WRITE_GUARDS[guard]}
        """
    written = []
    for work_id, raw, topic, subfield, field, domain, topics_json in updates:
        cur = conn.execute(sql, (topic, subfield, field, domain, topics_json, work_id, raw))
        if cur.rowcount == 1:
            written.append(work_id)
    conn.commit()
    return written


def run_pass(conn, rows, key, build_filter, mailto, batch_size, label, guard, scope_clause) -> tuple[int, int, int]:
    """Generic batched backfill. `key` maps a result work to its lookup token.

    rows: (work_id, token, raw identifier); ``guard`` and ``scope_clause`` go to ``write_results``.
    Rows that share a token (``W1`` and ``https://openalex.org/W1``) are all written; the token
    is requested once per batch. Progress counts rows.
    """
    processed = 0
    matched = 0
    fielded = 0
    total = len(rows)
    for batch in chunk(rows, batch_size):
        token_to_rows: dict[str, list[tuple[int, str]]] = {}
        for work_id, token, raw in batch:
            token_to_rows.setdefault(token, []).append((work_id, raw))
        try:
            data = openalex_request_json(
                endpoint="works",
                params={
                    "filter": build_filter(list(token_to_rows)),
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

        # One update per work, each with the raw value its row was read with: a second result
        # for the same token would otherwise fail the guard (the first already set
        # topics_fetched) and be counted as changed. The last result wins, as it did when every
        # result was written in turn.
        by_work_id = {}
        for work in data.get("results", []):
            for work_id, raw in token_to_rows.get(key(work), ()):
                by_work_id[work_id] = (work_id, raw, *topic_fields(work))
        updates = list(by_work_id.values())
        if updates:
            written = set(write_results(conn, updates, guard, scope_clause))
            matched += len(written)
            fielded += sum(1 for update in updates if update[0] in written and update[4])
            changed = len(updates) - len(written)
            if changed > 0:
                log(f"[{label}] {changed} work(s) changed while the batch was fetched; left for the next run")
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
            id_rows.append((row["id"], token, row["openalex_id"]))
    log(f"Pass 1 (by openalex_id): {len(id_rows)} works")
    run_pass(
        conn,
        id_rows,
        key=lambda work: normalize_openalex_id(work.get("id")),
        build_filter=lambda tokens: "openalex_id:" + "|".join(tokens),
        mailto=mailto,
        batch_size=batch_size,
        label="id",
        guard="openalex",
        scope_clause=scope_clause,
    )

    # Pass 2: matched works without an OpenAlex ID but with a DOI.
    if not args.no_doi_fallback:
        rows, skipped = doi_rows(conn, scope_clause=scope_clause, limit_clause=limit_clause)
        log(f"Pass 2 (by doi): {len(rows)} works, {skipped} skipped (not a whole DOI)")
        run_pass(
            conn,
            rows,
            key=lambda work: (bare_doi(work.get("doi")) or "").upper() or None,
            build_filter=doi_filter,
            mailto=mailto,
            batch_size=batch_size,
            label="doi",
            guard="doi",
            scope_clause=scope_clause,
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
