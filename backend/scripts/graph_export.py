import argparse
import hashlib
import json
import os
import re
import sqlite3
import time

STUB_GRAPH = {
    "nodes": [
        {
            "id": "W2175056322",
            "title": "The New Institutional Economics",
            "year": 2002,
            "type": "book-chapter",
            "status": "matched",
            "source_path": "seed_pdf",
            "ingest_source": "seed_pdf",
        },
        {
            "id": "W2015930340",
            "title": "The Nature of the Firm",
            "year": 1937,
            "type": "journal-article",
            "status": "matched",
            "source_path": "keyword_search",
            "ingest_source": "keyword_search",
        },
    ],
    "edges": [
        {"source": "W2175056322", "target": "W2015930340", "relationship_type": "references"},
    ],
}


def stable_signature(value):
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def normalize_openalex_id(value):
    if not value:
        return None
    match = re.search(r"(W\d+)", str(value), re.IGNORECASE)
    return match.group(1).upper() if match else None


def normalize_doi(value):
    if not value:
        return None
    match = re.search(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", str(value), re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return str(value).upper()


def derive_source_path_label(origin_key, source_pdf, run_id):
    origin = str(origin_key or "").strip()
    if origin:
        return origin
    source_pdf_value = str(source_pdf or "").strip()
    if source_pdf_value:
        return os.path.basename(source_pdf_value)
    if run_id is not None:
        return f"run:{run_id}"
    return "unknown"


def ensure_graph_cache_table(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS graph_exports (
            cache_key TEXT PRIMARY KEY,
            request_signature TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            payload_stats_json TEXT,
            nodes INTEGER DEFAULT 0,
            edges INTEGER DEFAULT 0,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_graph_exports_created_at ON graph_exports (created_at);
        """
    )


def build_cache_key(args):
    payload = {
        "max_nodes": args.max_nodes,
        "relationship": args.relationship,
        "status": args.status,
        "year_from": args.year_from,
        "year_to": args.year_to,
        "corpus_id": args.corpus_id,
        "run_id": args.run_id,
        "source": args.source,
        "hide_isolates": args.hide_isolates,
    }
    normalized = {key: stable_signature(value) for key, value in payload.items() if value not in (None, "")}
    signature = json.dumps(normalized, sort_keys=True)
    return hashlib.sha256(signature.encode("utf-8")).hexdigest(), signature


def try_read_cache(conn, cache_key, ttl_minutes):
    if ttl_minutes <= 0:
        return None
    now = int(time.time())
    row = conn.execute(
        "SELECT payload_json, created_at FROM graph_exports WHERE cache_key = ?",
        (cache_key,),
    ).fetchone()
    if not row:
        return None
    created_at = int(row["created_at"] or 0)
    if created_at + (int(ttl_minutes) * 60) < now:
        conn.execute("DELETE FROM graph_exports WHERE cache_key = ?", (cache_key,))
        conn.commit()
        return None
    try:
        return json.loads(row["payload_json"])
    except Exception:
        return None


def write_cache(conn, cache_key, signature, payload):
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO graph_exports (
            cache_key, request_signature, payload_json, payload_stats_json, nodes, edges, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            request_signature = excluded.request_signature,
            payload_json = excluded.payload_json,
            payload_stats_json = excluded.payload_stats_json,
            nodes = excluded.nodes,
            edges = excluded.edges,
            created_at = excluded.created_at
        """,
        (
            cache_key,
            signature,
            json.dumps(payload),
            json.dumps(payload.get("stats", {})),
            int(payload.get("stats", {}).get("node_count", 0)),
            int(payload.get("stats", {}).get("edge_count", 0)),
            now,
        ),
    )
    conn.commit()


def work_status(row):
    metadata = str(row.get("metadata_status") or "pending").strip().lower()
    download = str(row.get("download_status") or "not_requested").strip().lower()
    if download == "downloaded":
        return "downloaded"
    if download in {"queued", "in_progress"}:
        return "queued_download"
    if metadata == "in_progress":
        return "enriching"
    if metadata == "failed":
        return "failed_enrichment"
    if metadata == "matched":
        return "matched"
    return "raw"


def status_allowed(args_status, row):
    status = work_status(row)
    if args_status == "all":
        return True
    if args_status == "downloaded":
        return status == "downloaded"
    if args_status == "queued_download":
        return status == "queued_download"
    if args_status == "enriching":
        return status == "enriching"
    if args_status == "matched":
        return status == "matched"
    if args_status == "failed_enrichment":
        return status == "failed_enrichment"
    if args_status == "raw":
        return status == "raw"
    return False


def build_node(row):
    node_id = normalize_openalex_id(row.get("openalex_id")) or normalize_doi(row.get("doi")) or f"work:{row['id']}"
    return {
        "id": node_id,
        "title": row.get("title"),
        "year": row.get("year"),
        "type": row.get("type"),
        "status": work_status(row),
        "doi": row.get("doi"),
        "openalex_id": normalize_openalex_id(row.get("openalex_id")) or row.get("openalex_id"),
        "ingest_source": row.get("origin_key"),
        "source_pdf": row.get("source_pdf"),
        "source_path": derive_source_path_label(row.get("origin_key"), row.get("source_pdf"), row.get("run_id")),
        "_work_id": row.get("id"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--max-nodes", type=int, default=200)
    parser.add_argument("--relationship", choices=["references", "cited_by", "both"], default="both")
    parser.add_argument(
        "--status",
        choices=["downloaded", "queued_download", "enriching", "matched", "failed_enrichment", "raw", "all"],
        default="all",
    )
    parser.add_argument("--year-from", type=int, default=None)
    parser.add_argument("--year-to", type=int, default=None)
    parser.add_argument("--hide-isolates", type=int, default=1)
    parser.add_argument("--corpus-id", type=int, default=None)
    parser.add_argument("--run-id", type=int, default=None)
    parser.add_argument("--source", type=str, default=None)
    parser.add_argument("--cache-ttl-minutes", type=int, default=15)
    parser.add_argument("--force-refresh", action="store_true")
    args = parser.parse_args()

    if os.environ.get("RAG_FEEDER_STUB") == "1":
        print(json.dumps({**STUB_GRAPH, "source": "stub"}))
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    ensure_graph_cache_table(conn)

    cache_key, signature = build_cache_key(args)
    if not args.force_refresh:
        cached = try_read_cache(conn, cache_key, max(0, int(args.cache_ttl_minutes or 15)))
        if cached is not None:
            cached["source"] = "cache"
            conn.close()
            print(json.dumps(cached))
            return

    sql = """
        SELECT w.*
        FROM works w
    """
    params = []
    conditions = []
    if args.corpus_id is not None:
        sql += " JOIN corpus_works cw ON cw.work_id = w.id "
        conditions.append("cw.corpus_id = ?")
        params.append(args.corpus_id)
    if args.year_from is not None:
        conditions.append("(w.year IS NOT NULL AND w.year >= ?)")
        params.append(args.year_from)
    if args.year_to is not None:
        conditions.append("(w.year IS NOT NULL AND w.year <= ?)")
        params.append(args.year_to)
    if args.run_id is not None:
        conditions.append("w.run_id = ?")
        params.append(args.run_id)
    if args.source:
        conditions.append("LOWER(COALESCE(w.origin_key, '')) = LOWER(?)")
        params.append(args.source)
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    rows = [row for row in rows if status_allowed(args.status, row)]

    node_by_key = {}
    work_id_to_node_id = {}
    for row in rows:
        node = build_node(row)
        node_id = node["id"]
        work_id_to_node_id[row["id"]] = node_id
        node_by_key.setdefault(node_id, node)

    if not node_by_key:
        payload = {
            "nodes": [],
            "edges": [],
            "stats": {"node_count": 0, "edge_count": 0, "relationship_counts": {"references": 0, "cited_by": 0}},
        }
        write_cache(conn, cache_key, signature, payload)
        conn.close()
        print(json.dumps(payload))
        return

    allowed_node_ids = set(node_by_key.keys())
    edges = []
    edge_set = set()

    if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='citation_edges'").fetchone():
        for row in conn.execute("SELECT source_id, target_id, relationship_type, run_id FROM citation_edges").fetchall():
            source_id = row["source_id"]
            target_id = row["target_id"]
            relationship = row["relationship_type"] or "references"
            if args.relationship != "both" and relationship != args.relationship:
                continue
            if args.run_id is not None and row["run_id"] is not None:
                try:
                    if int(row["run_id"]) != int(args.run_id):
                        continue
                except Exception:
                    continue
            if source_id in allowed_node_ids and target_id in allowed_node_ids and source_id != target_id:
                key = (source_id, target_id, relationship)
                if key not in edge_set:
                    edge_set.add(key)
                    edges.append({"source": source_id, "target": target_id, "relationship_type": relationship})

    if not edges:
        for row in rows:
            source_work_id = row.get("source_work_id")
            if not source_work_id:
                continue
            source_node = work_id_to_node_id.get(source_work_id)
            if not source_node:
                source_node = work_id_to_node_id.get(str(source_work_id))
            if not source_node:
                normalized_source = normalize_openalex_id(source_work_id)
                if normalized_source and normalized_source in allowed_node_ids:
                    source_node = normalized_source
            if not source_node:
                normalized_source = normalize_doi(source_work_id)
                if normalized_source and normalized_source in allowed_node_ids:
                    source_node = normalized_source
            target_node = work_id_to_node_id.get(row["id"])
            if not source_node or not target_node or source_node == target_node:
                continue
            relationship = row.get("relationship_type") or "references"
            if args.relationship != "both" and relationship != args.relationship:
                continue
            key = (source_node, target_node, relationship)
            if key not in edge_set:
                edge_set.add(key)
                edges.append({"source": source_node, "target": target_node, "relationship_type": relationship})

    degree = {}
    for edge in edges:
        degree[edge["source"]] = degree.get(edge["source"], 0) + 1
        degree[edge["target"]] = degree.get(edge["target"], 0) + 1

    nodes = list(node_by_key.values())
    if args.hide_isolates:
        nodes = [node for node in nodes if degree.get(node["id"], 0) > 0]
        node_ids = {node["id"] for node in nodes}
        edges = [edge for edge in edges if edge["source"] in node_ids and edge["target"] in node_ids]

    nodes.sort(key=lambda item: (-(degree.get(item["id"], 0)), item.get("title") or "", item["id"]))
    if args.max_nodes > 0 and len(nodes) > args.max_nodes:
        keep = {node["id"] for node in nodes[: args.max_nodes]}
        nodes = [node for node in nodes if node["id"] in keep]
        edges = [edge for edge in edges if edge["source"] in keep and edge["target"] in keep]

    for node in nodes:
        node["degree"] = degree.get(node["id"], 0)
        node.pop("_work_id", None)

    payload = {
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "relationship_counts": {
                "references": sum(1 for edge in edges if edge.get("relationship_type") == "references"),
                "cited_by": sum(1 for edge in edges if edge.get("relationship_type") == "cited_by"),
            },
        },
    }
    write_cache(conn, cache_key, signature, payload)
    conn.close()
    print(json.dumps(payload))


if __name__ == "__main__":
    main()
