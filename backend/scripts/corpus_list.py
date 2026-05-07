import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path


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


def table_exists(conn, table_name):
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def load_pdf_source_labels(conn, corpus_id):
    if not table_exists(conn, "ingest_source_metadata"):
        return {}
    rows = conn.execute(
        """
        SELECT corpus_id, ingest_source, title, source_pdf
        FROM ingest_source_metadata
        WHERE (? IS NULL OR corpus_id = ?)
        """,
        (corpus_id, corpus_id),
    ).fetchall()
    labels = {}
    for row in rows:
        key = str(row["ingest_source"] or "").strip()
        if not key:
            continue
        label = row["title"] or (Path(str(row["source_pdf"])).stem if row["source_pdf"] else "") or key
        labels[(row["corpus_id"], key)] = label
        labels[(None, key)] = label
    return labels


def load_search_source_labels(conn):
    if not table_exists(conn, "search_runs"):
        return {}
    rows = conn.execute("SELECT id, query FROM search_runs").fetchall()
    return {
        str(row["id"]): (row["query"] or f"Search #{row['id']}")
        for row in rows
    }


def load_seed_provenance(conn, corpus_id, pdf_source_labels, search_source_labels):
    if not table_exists(conn, "seed_candidates_in_corpus"):
        return {}
    rows = conn.execute(
        """
        SELECT work_id, source_type, source_key, marked_at
        FROM seed_candidates_in_corpus
        WHERE corpus_id = ?
          AND work_id IS NOT NULL
        ORDER BY marked_at DESC
        """,
        (corpus_id,),
    ).fetchall()
    provenance = {}
    for row in rows:
        work_id = normalize_id(row["work_id"])
        if not work_id or work_id in provenance:
            continue
        source_type = str(row["source_type"] or "").strip().lower()
        source_key = str(row["source_key"] or "").strip()
        if source_type == "pdf":
            label = pdf_source_labels.get((corpus_id, source_key)) or pdf_source_labels.get((None, source_key)) or source_key
        elif source_type == "search":
            label = search_source_labels.get(source_key) or f"Search #{source_key}"
        else:
            label = source_key
        provenance[work_id] = {
            "source_type": source_type,
            "source_key": source_key,
            "source_label": label,
        }
    return provenance


def source_label_for_work(data, corpus_id, pdf_source_labels, search_source_labels, seed_provenance):
    work_id = normalize_id(data.get("id"))
    if work_id in seed_provenance:
        return seed_provenance[work_id]

    origin_key = str(data.get("origin_key") or "").strip()
    origin_type = str(data.get("origin_type") or "").strip().lower()
    run_id = data.get("run_id")

    if origin_type == "bibtex_import" or origin_key.endswith("/metadata.bib"):
        return {"source_type": "", "source_key": "", "source_label": ""}

    if origin_key.startswith("search:"):
        search_key = origin_key.split(":", 1)[1]
        return {
            "source_type": "search",
            "source_key": search_key,
            "source_label": search_source_labels.get(search_key) or f"Search #{search_key}",
        }

    if run_id is not None:
        search_key = str(run_id)
        return {
            "source_type": "search",
            "source_key": search_key,
            "source_label": search_source_labels.get(search_key) or f"Search #{search_key}",
        }

    if origin_key:
        label = pdf_source_labels.get((corpus_id, origin_key)) or pdf_source_labels.get((None, origin_key))
        if label:
            return {"source_type": "pdf", "source_key": origin_key, "source_label": label}
        return {"source_type": origin_type or "source", "source_key": origin_key, "source_label": origin_key}

    source_pdf = data.get("source_pdf")
    if source_pdf:
        return {"source_type": "pdf", "source_key": str(source_pdf), "source_label": Path(str(source_pdf)).stem}

    source = data.get("metadata_source") or data.get("source") or ""
    return {"source_type": "", "source_key": "", "source_label": source}


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
    pdf_source_labels = load_pdf_source_labels(conn, args.corpus_id)
    search_source_labels = load_search_source_labels(conn)
    seed_provenance = load_seed_provenance(conn, args.corpus_id, pdf_source_labels, search_source_labels)

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
        source_info = source_label_for_work(data, args.corpus_id, pdf_source_labels, search_source_labels, seed_provenance)
        items.append(
            {
                "id": data.get("id"),
                "work_id": data.get("id"),
                "title": data.get("title"),
                "authors": data.get("authors"),
                "year": data.get("year"),
                "source": source,
                "source_label": source_info["source_label"],
                "source_type": source_info["source_type"],
                "source_key": source_info["source_key"],
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
