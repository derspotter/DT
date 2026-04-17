import argparse
import json
import sqlite3
from collections import Counter


STUB_ITEMS = [
    {
        "id": 1,
        "title": "The New Institutional Economics",
        "year": 2002,
        "source": "Seed bibliography",
        "status": "matched",
        "doi": "10.1017/cbo9780511613807.002",
        "openalex_id": "W2175056322",
    },
    {
        "id": 2,
        "title": "Markets and Hierarchies",
        "year": 1975,
        "source": "Keyword search",
        "status": "queued_download",
        "doi": None,
        "openalex_id": "W1974636593",
    },
]


def status_from_row(data):
    metadata = str(data.get("metadata_status") or "pending").strip().lower()
    download = str(data.get("download_status") or "not_requested").strip().lower()
    if download == "downloaded":
        return "downloaded"
    if download in {"queued", "in_progress"}:
        return "queued_download"
    if download == "failed":
        return "failed_download"
    if metadata == "in_progress":
        return "enriching"
    if metadata == "failed":
        return "failed_enrichment"
    if metadata == "matched":
        return "matched"
    return "raw"


def normalize_year(value):
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def normalize_id(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--corpus-id", type=int, default=None)
    args = parser.parse_args()

    if "RAG_FEEDER_STUB" in __import__("os").environ:
        print(
            json.dumps(
                {
                    "items": STUB_ITEMS,
                    "total": len(STUB_ITEMS),
                    "source": "stub",
                    "stage_totals": {
                        "raw": sum(1 for item in STUB_ITEMS if item.get("status") in ("raw", "extract_references_from_pdf", "pending")),
                        "metadata": sum(1 for item in STUB_ITEMS if item.get("status") in ("matched", "queued_download", "enriching")),
                        "downloaded": sum(1 for item in STUB_ITEMS if item.get("status") == "downloaded"),
                        "failed_enrichment": sum(1 for item in STUB_ITEMS if item.get("status") == "failed_enrichment"),
                        "failed_download": sum(1 for item in STUB_ITEMS if item.get("status") == "failed_download"),
                    },
                    "status_counts": dict(Counter(item.get("status") for item in STUB_ITEMS)),
                }
            )
        )
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    if args.corpus_id is not None:
        rows = conn.execute(
            """
            SELECT w.*
            FROM works w
            JOIN corpus_works cw ON cw.work_id = w.id
            WHERE cw.corpus_id = ?
            """,
            (args.corpus_id,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM works").fetchall()

    items = []
    for row in rows:
        data = dict(row)
        status = status_from_row(data)
        source = data.get("origin_key") or data.get("source_pdf") or data.get("metadata_source") or data.get("source")
        items.append(
            {
                "id": data.get("id"),
                "work_id": data.get("id"),
                "title": data.get("title"),
                "authors": data.get("authors"),
                "year": data.get("year"),
                "source": source,
                "status": status,
                "metadata_status": data.get("metadata_status"),
                "download_status": data.get("download_status"),
                "doi": data.get("doi"),
                "openalex_id": data.get("openalex_id"),
                "file_path": data.get("file_path"),
            }
        )

    conn.close()

    status_counts = Counter(item.get("status") for item in items)
    stage_totals = {
        "raw": sum(status_counts.get(status, 0) for status in ("raw", "extract_references_from_pdf", "pending")),
        "metadata": sum(status_counts.get(status, 0) for status in ("matched", "queued_download", "enriching")),
        "downloaded": status_counts.get("downloaded", 0),
        "failed_enrichment": status_counts.get("failed_enrichment", 0),
        "failed_download": status_counts.get("failed_download", 0),
    }

    items.sort(key=lambda x: (normalize_year(x.get("year")), normalize_id(x.get("id"))), reverse=True)
    total = len(items)
    start = max(args.offset, 0)
    end = start + max(args.limit, 0)
    items = items[start:end]

    print(
        json.dumps(
            {
                "items": items,
                "total": total,
                "stage_totals": stage_totals,
                "status_counts": dict(status_counts),
            }
        )
    )


if __name__ == "__main__":
    main()
