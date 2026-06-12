"""Backfill in-corpus citation edges from OpenAlex `referenced_works`.

Only ~1% of works ever had their references extracted, so the citation graph is
artificially sparse and most works appear isolated even though OpenAlex knows
they cite ~38 works each. This fetches each work's `referenced_works` (via
non-billable openalex_id lookups) and records a citation edge for every
referenced work that is also in the corpus.

Processing every work's references captures the complete in-corpus citation
graph (every edge A->B is found when A is processed), so a separate cited_by
pass is unnecessary.

Usage:
    python backend/scripts/backfill_citation_edges.py --db-path <path> [--limit N] [--scope matched|all]
"""

import argparse
import re
import sqlite3
import sys
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

OPENALEX_OR_LIMIT = 50
RUN_ID = "openalex_refs_backfill"


def log(message):
    print(message, file=sys.stderr, flush=True)


def normalize_openalex_id(value):
    match = re.search(r"(W\d+)", str(value or ""), re.IGNORECASE)
    return match.group(1).upper() if match else None


def ensure_refs_fetched_column(conn):
    columns = {row[1] for row in conn.execute("PRAGMA table_info(works)")}
    if "refs_fetched" not in columns:
        conn.execute("ALTER TABLE works ADD COLUMN refs_fetched INTEGER")
        conn.commit()
        log("Added column: refs_fetched")


def undirected(source, target):
    return (source, target) if source <= target else (target, source)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--batch-size", type=int, default=OPENALEX_OR_LIMIT)
    parser.add_argument("--limit", type=int, default=0, help="cap works processed (0 = all)")
    parser.add_argument("--scope", choices=["matched", "all"], default="matched")
    args = parser.parse_args()

    batch_size = max(1, min(OPENALEX_OR_LIMIT, args.batch_size))
    mailto = get_openalex_mailto()

    conn = sqlite3.connect(args.db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 60000")
    ensure_refs_fetched_column(conn)

    scope_clause = (
        "metadata_status = 'matched' AND download_status = 'downloaded'"
        if args.scope == "matched"
        else "openalex_id IS NOT NULL"
    )

    # Corpus: OpenAlex id -> work id. Edges are only kept between corpus works.
    corpus = {}
    for row in conn.execute(
        f"SELECT id, openalex_id FROM works WHERE {scope_clause} AND openalex_id IS NOT NULL AND openalex_id <> ''"
    ):
        token = normalize_openalex_id(row["openalex_id"])
        if token:
            corpus[token] = row["id"]
    log(f"corpus works with openalex_id: {len(corpus)}")

    # Existing edges (undirected) so we never insert a duplicate link.
    existing_pairs = set()
    for row in conn.execute("SELECT source_id, target_id FROM citation_edges"):
        existing_pairs.add(undirected(row["source_id"], row["target_id"]))
    log(f"existing edges: {len(existing_pairs)}")

    limit_clause = f" LIMIT {int(args.limit)}" if args.limit else ""
    todo = []
    for row in conn.execute(
        f"""
        SELECT id, openalex_id FROM works
        WHERE {scope_clause}
          AND openalex_id IS NOT NULL AND openalex_id <> ''
          AND (refs_fetched IS NULL OR refs_fetched = 0)
        ORDER BY id{limit_clause}
        """
    ):
        token = normalize_openalex_id(row["openalex_id"])
        if token:
            todo.append((row["id"], token))
    log(f"works to process: {len(todo)}")

    processed = 0
    added = 0
    for offset in range(0, len(todo), batch_size):
        chunk = todo[offset : offset + batch_size]
        work_id_by_token = {token: work_id for work_id, token in chunk}
        try:
            data = openalex_request_json(
                endpoint="works",
                params={
                    "filter": "openalex_id:" + "|".join(token for _wid, token in chunk),
                    "per-page": batch_size,
                    "select": "id,referenced_works",
                },
                mailto=mailto,
            )
        except OpenAlexRateLimitExceeded as exc:
            log(f"rate limited, stopping early: {exc}")
            break
        except Exception as exc:  # network/HTTP: skip batch, keep going
            log(f"batch error ({exc}); skipping {len(chunk)} works")
            processed += len(chunk)
            continue

        new_rows = []
        seen_work_ids = []
        for work in data.get("results", []):
            source_token = normalize_openalex_id(work.get("id"))
            source_work_id = work_id_by_token.get(source_token)
            if source_work_id is None:
                continue
            seen_work_ids.append(source_work_id)
            for reference in work.get("referenced_works") or []:
                target_token = normalize_openalex_id(reference)
                if not target_token or target_token == source_token or target_token not in corpus:
                    continue
                pair = undirected(source_token, target_token)
                if pair in existing_pairs:
                    continue
                existing_pairs.add(pair)
                new_rows.append(
                    (source_token, target_token, "references", "works", "works",
                     source_work_id, corpus[target_token], RUN_ID)
                )
        if new_rows:
            conn.executemany(
                """
                INSERT INTO citation_edges
                  (source_id, target_id, relationship_type, source_table, target_table,
                   source_row_id, target_row_id, run_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                new_rows,
            )
            added += len(new_rows)
        if seen_work_ids:
            conn.executemany(
                "UPDATE works SET refs_fetched = 1 WHERE id = ?",
                [(work_id,) for work_id in seen_work_ids],
            )
        conn.commit()
        processed += len(chunk)
        log(f"{processed}/{len(todo)} processed, {added} edges added")

    log(f"Done. Added {added} in-corpus citation edges (run_id={RUN_ID}).")
    conn.close()


if __name__ == "__main__":
    main()
