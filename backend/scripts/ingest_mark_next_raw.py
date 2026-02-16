import argparse
import json
import sqlite3


def _fetch_candidates(cur, corpus_id: int | None, limit: int) -> list[int]:
    if corpus_id is None:
        cur.execute(
            """
            SELECT id
            FROM no_metadata
            WHERE COALESCE(selected_for_enrichment, 0) = 0
            ORDER BY id
            LIMIT ?
            """,
            (limit,),
        )
    else:
        cur.execute(
            """
            SELECT nm.id
            FROM no_metadata nm
            JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = nm.id
            WHERE ci.corpus_id = ?
              AND COALESCE(nm.selected_for_enrichment, 0) = 0
            ORDER BY nm.id
            LIMIT ?
            """,
            (corpus_id, limit),
        )
    return [int(row[0]) for row in cur.fetchall()]


def _count_available(cur, corpus_id: int | None) -> int:
    if corpus_id is None:
        cur.execute("SELECT COUNT(*) FROM no_metadata WHERE COALESCE(selected_for_enrichment, 0) = 0")
        return int(cur.fetchone()[0])
    cur.execute(
        """
        SELECT COUNT(*)
        FROM no_metadata nm
        JOIN corpus_items ci ON ci.table_name = 'no_metadata' AND ci.row_id = nm.id
        WHERE ci.corpus_id = ?
          AND COALESCE(nm.selected_for_enrichment, 0) = 0
        """,
        (corpus_id,),
    )
    return int(cur.fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mark next unselected raw entries for enrichment (selected_for_enrichment=1)."
    )
    parser.add_argument("--db-path", required=True, help="Path to SQLite DB.")
    parser.add_argument("--limit", type=int, default=10, help="Maximum rows to mark.")
    parser.add_argument("--corpus-id", type=int, default=None, help="Optional corpus scope.")
    args = parser.parse_args()

    limit = max(0, int(args.limit or 0))
    if limit <= 0:
        print(
            json.dumps(
                {"requested": int(args.limit or 0), "marked": 0, "available": 0, "ids": [], "errors": ["limit must be > 0"]}
            )
        )
        return

    conn = sqlite3.connect(args.db_path)
    cur = conn.cursor()

    available = _count_available(cur, args.corpus_id)
    ids = _fetch_candidates(cur, args.corpus_id, limit)
    if ids:
        placeholders = ",".join("?" for _ in ids)
        cur.execute(
            f"UPDATE no_metadata SET selected_for_enrichment = 1 WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
    conn.close()

    print(
        json.dumps(
            {
                "requested": limit,
                "marked": len(ids),
                "available": available,
                "ids": ids,
                "source": "db",
            }
        )
    )


if __name__ == "__main__":
    main()
