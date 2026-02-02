import json
from dataclasses import dataclass
from typing import Iterable

import requests

from .OpenAlexScraper import (
    OpenAlexCrossrefSearcher,
    fetch_referenced_work_details,
    fetch_citing_work_ids,
)
from .db_manager import DatabaseManager
from .utils import get_global_rate_limiter


def normalize_openalex_id(value: str | None) -> str | None:
    if not value:
        return None
    if value.startswith("http"):
        value = value.rstrip("/").split("/")[-1]
    return value if value.startswith("W") else None


@dataclass
class ExpansionStats:
    processed: int = 0
    skipped_existing: int = 0
    skipped_invalid: int = 0
    fetched: int = 0
    added_refs: int = 0
    added_citations: int = 0
    errors: int = 0


def fetch_openalex_work(
    openalex_id: str,
    *,
    searcher: OpenAlexCrossrefSearcher,
    rate_limiter,
    mailto: str,
) -> dict | None:
    work_id = normalize_openalex_id(openalex_id)
    if not work_id:
        return None
    url = f"{searcher.base_url}/{work_id}"
    params = {"select": searcher.fields, "mailto": mailto}
    try:
        rate_limiter.wait_if_needed("openalex")
        response = requests.get(url, headers=searcher.headers, params=params, timeout=20)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"[OpenAlex] Failed to fetch {openalex_id}: {exc}")
        return None


def _iter_target_rows(
    cursor,
    *,
    include_related: bool,
    limit: int,
    offset: int,
) -> Iterable[tuple[int, str]]:
    query = [
        "SELECT id, openalex_id FROM with_metadata",
        "WHERE openalex_id IS NOT NULL AND openalex_id != ''",
    ]
    if not include_related:
        query.append("AND (relationship_type IS NULL OR relationship_type = '')")
    query.append("ORDER BY id LIMIT ? OFFSET ?")
    sql = " ".join(query)
    for row in cursor.execute(sql, (limit, offset)):
        yield row[0], row[1]


def expand_openalex_links(
    *,
    db_path: str | None = None,
    db_manager: DatabaseManager | None = None,
    mailto: str,
    limit: int = 50,
    offset: int = 0,
    include_related: bool = False,
    fetch_references: bool = True,
    fetch_citations: bool = False,
    max_citations: int = 50,
    force: bool = False,
    update_openalex_json: bool = True,
) -> ExpansionStats:
    own_manager = False
    if db_manager is None:
        if not db_path:
            raise ValueError("db_path is required when db_manager is not provided.")
        db_manager = DatabaseManager(db_path)
        own_manager = True

    stats = ExpansionStats()
    searcher = OpenAlexCrossrefSearcher(mailto=mailto)
    limiter = get_global_rate_limiter()

    try:
        select_cursor = db_manager.conn.cursor()
        rows = list(
            _iter_target_rows(
                select_cursor,
                include_related=include_related,
                limit=limit,
                offset=offset,
            )
        )
        op_cursor = db_manager.conn.cursor()
        for row_id, openalex_id in rows:
            stats.processed += 1
            normalized_id = normalize_openalex_id(openalex_id)
            if not normalized_id:
                stats.skipped_invalid += 1
                continue

            if not force:
                edge_count = op_cursor.execute(
                    "SELECT COUNT(*) FROM citation_edges WHERE source_row_id = ?",
                    (row_id,),
                ).fetchone()[0]
                if edge_count:
                    stats.skipped_existing += 1
                    continue

            work = fetch_openalex_work(
                normalized_id,
                searcher=searcher,
                rate_limiter=limiter,
                mailto=mailto,
            )
            if not work:
                stats.errors += 1
                continue
            stats.fetched += 1

            if update_openalex_json:
                op_cursor.execute(
                    "UPDATE with_metadata SET openalex_json = ? WHERE id = ?",
                    (json.dumps(work), row_id),
                )

            if fetch_references and work.get("referenced_works"):
                refs = fetch_referenced_work_details(
                    work.get("referenced_works", []),
                    limiter,
                    mailto,
                )
                if refs:
                    stats.added_refs += db_manager.add_referenced_works_to_with_metadata(row_id, refs)

            if fetch_citations and work.get("cited_by_api_url"):
                citations = fetch_citing_work_ids(
                    work.get("cited_by_api_url"),
                    limiter,
                    mailto,
                    max_citations=max_citations,
                )
                if citations:
                    stats.added_citations += db_manager.add_citing_works_to_with_metadata(row_id, citations)

            db_manager.conn.commit()

    finally:
        if own_manager:
            db_manager.close_connection()

    return stats
