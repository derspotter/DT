import argparse
import json
import os
import sqlite3


STUB_ITEMS = [
    {
        "id": 101,
        "title": "The New Institutional Economics",
        "status": "queued",
        "attempts": 0,
        "downloaded_at": None,
    },
    {
        "id": 102,
        "title": "Markets and Hierarchies",
        "status": "in_progress",
        "attempts": 1,
        "downloaded_at": None,
    },
    {
        "id": 103,
        "title": "The Nature of the Firm",
        "status": "downloaded",
        "attempts": 1,
        "downloaded_at": "2026-04-08T10:00:00",
    },
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--corpus-id", type=int, default=None)
    args = parser.parse_args()

    if os.environ.get("RAG_FEEDER_STUB") == "1":
        print(json.dumps({"items": STUB_ITEMS, "total": len(STUB_ITEMS), "source": "stub"}))
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    items = []
    try:
        params = []
        corpus_join = ''
        corpus_where = ''
        if args.corpus_id is not None:
            corpus_join = 'JOIN corpus_works cw ON cw.work_id = w.id'
            corpus_where = 'cw.corpus_id = ? AND'
            params.append(args.corpus_id)

        rows = conn.execute(
            f"""
            SELECT w.id, w.title, w.download_status, w.download_attempt_count, w.downloaded_at
            FROM works w
            {corpus_join}
            WHERE {corpus_where} w.download_status IN ('queued', 'in_progress', 'downloaded')
            ORDER BY
              CASE w.download_status
                WHEN 'in_progress' THEN 0
                WHEN 'queued' THEN 1
                WHEN 'downloaded' THEN 2
                ELSE 9
              END,
              CASE WHEN w.download_status = 'downloaded' THEN datetime(w.downloaded_at) END DESC,
              w.id DESC
            """,
            params,
        ).fetchall()

        for row in rows:
            data = dict(row)
            items.append(
                {
                    "id": data.get("id"),
                    "title": data.get("title"),
                    "status": (data.get("download_status") or "").strip() or "queued",
                    "attempts": int(data.get("download_attempt_count") or 0),
                    "downloaded_at": data.get("downloaded_at"),
                }
            )
    except sqlite3.Error:
        items = []
    finally:
        conn.close()

    total = len(items)
    start = max(args.offset, 0)
    end = start + max(args.limit, 0)
    items = items[start:end]

    print(json.dumps({"items": items, "total": total}))


if __name__ == "__main__":
    main()
