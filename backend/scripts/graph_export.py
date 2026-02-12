import argparse
import json
import sqlite3
import os
import re

STUB_GRAPH = {
    'nodes': [
        { 'id': 'W2175056322', 'title': 'The New Institutional Economics', 'year': 2002, 'type': 'book-chapter' },
        { 'id': 'W2015930340', 'title': 'The Nature of the Firm', 'year': 1937, 'type': 'journal-article' },
    ],
    'edges': [
        { 'source': 'W2175056322', 'target': 'W2015930340', 'relationship_type': 'references' },
    ],
}

def normalize_openalex_id(value):
    if not value:
        return None
    match = re.search(r"(W\\d+)", str(value), re.IGNORECASE)
    return match.group(1).upper() if match else None

def normalize_doi(value):
    if not value:
        return None
    match = re.search(r"(10\\.\\d{4,9}/[-._;()/:A-Z0-9]+)", str(value), re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return str(value).upper()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-path', required=True)
    parser.add_argument('--max-nodes', type=int, default=200)
    parser.add_argument('--relationship', choices=['references', 'cited_by', 'both'], default='both')
    parser.add_argument('--status', choices=['downloaded', 'queued', 'with_metadata', 'all'], default='all')
    parser.add_argument('--year-from', type=int, default=None)
    parser.add_argument('--year-to', type=int, default=None)
    parser.add_argument('--hide-isolates', type=int, default=1)
    parser.add_argument('--corpus-id', type=int, default=None)
    args = parser.parse_args()

    if os.environ.get('RAG_FEEDER_STUB') == '1':
        print(json.dumps({ **STUB_GRAPH, 'source': 'stub' }))
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    allowed_rows = None
    if args.corpus_id is not None:
        allowed_rows = {}
        try:
            rows = conn.execute(
                "SELECT table_name, row_id FROM corpus_items WHERE corpus_id = ?",
                (args.corpus_id,),
            ).fetchall()
            for row in rows:
                allowed_rows.setdefault(row["table_name"], set()).add(row["row_id"])
        except sqlite3.Error:
            allowed_rows = None

    tables = []
    if args.status in ('all', 'with_metadata', 'queued'):
        tables.append(('with_metadata', 'with_metadata'))
    if args.status in ('all', 'downloaded'):
        tables.append(('downloaded_references', 'downloaded'))
    # queued rows now live in with_metadata (download_state in queued/in_progress)

    node_map = {}
    node_by_id = {}
    nodes = []
    edges = []

    def has_table(name: str) -> bool:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None

    def add_node(table, row, status):
        raw_id = normalize_openalex_id(row.get('openalex_id')) or normalize_doi(row.get('doi')) or f"{table}:{row.get('id')}"
        node_key = f"{table}:{row.get('id')}"
        if node_key in node_map:
            return node_map[node_key]
        if raw_id in node_by_id:
            node = node_by_id[raw_id]
            existing_status = node.get('status')
            if isinstance(existing_status, list):
                if status not in existing_status:
                    existing_status.append(status)
            elif existing_status and existing_status != status:
                node['status'] = sorted({existing_status, status})
            elif not existing_status:
                node['status'] = status
            node_map[node_key] = raw_id
            return raw_id

        node = {
            'id': raw_id,
            'title': row.get('title'),
            'year': row.get('year'),
            'type': row.get('entry_type') or row.get('type'),
            'status': status,
        }
        node_by_id[raw_id] = node
        node_map[node_key] = raw_id
        nodes.append(node)
        return raw_id

    def include_row(data):
        year = data.get('year')
        if args.year_from is not None and (year is None or year < args.year_from):
            return False
        if args.year_to is not None and (year is None or year > args.year_to):
            return False
        return True

    wanted_ids = None
    edge_rows = []
    use_edge_table = False
    if has_table("citation_edges") and args.status in ("all", "with_metadata"):
        try:
            raw_edges = conn.execute(
                "SELECT source_id, target_id, relationship_type FROM citation_edges"
            ).fetchall()
        except sqlite3.Error:
            raw_edges = []

        if raw_edges:
            allowed_node_ids = None
            if allowed_rows is not None:
                allowed_meta_ids = allowed_rows.get("with_metadata", set())
                allowed_node_ids = set()
                if allowed_meta_ids:
                    meta_rows = conn.execute(
                        "SELECT id, openalex_id, doi FROM with_metadata"
                    ).fetchall()
                    for row in meta_rows:
                        if row["id"] not in allowed_meta_ids:
                            continue
                        norm_openalex = normalize_openalex_id(row["openalex_id"])
                        norm_doi = normalize_doi(row["doi"])
                        if norm_openalex:
                            allowed_node_ids.add(norm_openalex)
                        if norm_doi:
                            allowed_node_ids.add(norm_doi)

            degree_counts = {}
            filtered_edges = []
            for row in raw_edges:
                rel = row["relationship_type"]
                if args.relationship != "both" and rel != args.relationship:
                    continue
                source_id = row["source_id"]
                target_id = row["target_id"]
                if source_id == target_id:
                    continue
                if allowed_node_ids is not None:
                    if source_id not in allowed_node_ids or target_id not in allowed_node_ids:
                        continue
                filtered_edges.append((source_id, target_id, rel))
                degree_counts[source_id] = degree_counts.get(source_id, 0) + 1
                degree_counts[target_id] = degree_counts.get(target_id, 0) + 1

            if filtered_edges:
                adjacency = {}
                for source_id, target_id, rel in filtered_edges:
                    adjacency.setdefault(source_id, set()).add(target_id)
                    adjacency.setdefault(target_id, set()).add(source_id)

                top_nodes = sorted(
                    degree_counts.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
                seed_count = max(5, min(50, max(1, args.max_nodes // 10)))
                wanted_ids = set()
                queue = []
                for node_id, _count in top_nodes[:seed_count]:
                    if node_id in wanted_ids:
                        continue
                    wanted_ids.add(node_id)
                    queue.append(node_id)

                queue_index = 0
                while queue_index < len(queue) and len(wanted_ids) < args.max_nodes:
                    current = queue[queue_index]
                    queue_index += 1
                    for neighbor in adjacency.get(current, []):
                        if neighbor in wanted_ids:
                            continue
                        wanted_ids.add(neighbor)
                        queue.append(neighbor)
                        if len(wanted_ids) >= args.max_nodes:
                            break

                for source_id, target_id, rel in filtered_edges:
                    if source_id in wanted_ids and target_id in wanted_ids:
                        edge_rows.append({
                            "source": source_id,
                            "target": target_id,
                            "relationship_type": rel,
                        })
                if wanted_ids:
                    use_edge_table = True

    if use_edge_table:
        meta_rows = conn.execute(
            "SELECT id, title, year, type, openalex_id, doi FROM with_metadata"
        ).fetchall()
        meta_by_id = {}
        for row in meta_rows:
            if allowed_rows is not None:
                allowed_meta_ids = allowed_rows.get("with_metadata", set())
                if row["id"] not in allowed_meta_ids:
                    continue
            norm_openalex = normalize_openalex_id(row["openalex_id"])
            norm_doi = normalize_doi(row["doi"])
            if norm_openalex:
                meta_by_id[norm_openalex] = row
            if norm_doi:
                meta_by_id[norm_doi] = row

        for node_id in sorted(wanted_ids):
            meta = meta_by_id.get(node_id)
            node = {
                "id": node_id,
                "title": meta["title"] if meta else node_id,
                "year": meta["year"] if meta else None,
                "type": meta["type"] if meta else None,
                "status": "with_metadata" if meta else "unknown",
            }
            nodes.append(node)
            node_by_id[node_id] = node

        edges = edge_rows
    else:
        all_rows = []
        for table, status in tables:
            try:
                rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            except sqlite3.Error:
                continue
            for row in rows:
                if len(nodes) >= args.max_nodes:
                    break
                data = dict(row)
                if allowed_rows is not None:
                    allowed_ids = allowed_rows.get(table, set())
                    if data.get("id") not in allowed_ids:
                        continue
                if table == 'with_metadata':
                    state = (data.get('download_state') or '').strip().lower()
                    if args.status == 'queued' and state not in ('queued', 'in_progress'):
                        continue
                    if args.status == 'with_metadata' and state in ('queued', 'in_progress'):
                        continue
                if not include_row(data):
                    continue
                raw_id = normalize_openalex_id(data.get("openalex_id")) or normalize_doi(data.get("doi")) or f"{table}:{data.get('id')}"
                if wanted_ids is not None and raw_id not in wanted_ids:
                    continue
                node_status = status
                if table == 'with_metadata' and (data.get('download_state') or '').strip().lower() in ('queued', 'in_progress'):
                    node_status = 'queued'
                all_rows.append((table, node_status, data))
                add_node(table, data, node_status)
            if len(nodes) >= args.max_nodes:
                break

        if edge_rows:
            edges = [
                edge for edge in edge_rows
                if edge["source"] in node_by_id and edge["target"] in node_by_id
            ]
        else:
            for table, _status, data in all_rows:
                node_id = node_map.get(f"{table}:{data.get('id')}")
                if not node_id:
                    continue
                source_work_id = data.get('source_work_id')
                if not source_work_id:
                    continue
                source_node = node_map.get(f"{table}:{source_work_id}")
                if not source_node:
                    continue
                relationship_type = data.get('relationship_type') or 'references'
                if args.relationship != 'both' and relationship_type != args.relationship:
                    continue
                edges.append({
                    'source': source_node,
                    'target': node_id,
                    'relationship_type': relationship_type,
                })

    conn.close()

    degree = {}
    for edge in edges:
        degree[edge['source']] = degree.get(edge['source'], 0) + 1
        degree[edge['target']] = degree.get(edge['target'], 0) + 1

    if args.hide_isolates:
        nodes = [node for node in nodes if degree.get(node['id'], 0) > 0]
        node_ids = {node['id'] for node in nodes}
        edges = [edge for edge in edges if edge['source'] in node_ids and edge['target'] in node_ids]
        degree = {}
        for edge in edges:
            degree[edge['source']] = degree.get(edge['source'], 0) + 1
            degree[edge['target']] = degree.get(edge['target'], 0) + 1

    for node in nodes:
        node['degree'] = degree.get(node['id'], 0)

    print(json.dumps({
        'nodes': nodes,
        'edges': edges,
        'stats': {
            'node_count': len(nodes),
            'edge_count': len(edges),
            'relationship_counts': {
                'references': sum(1 for edge in edges if edge.get('relationship_type') == 'references'),
                'cited_by': sum(1 for edge in edges if edge.get('relationship_type') == 'cited_by'),
            },
        }
    }))


if __name__ == '__main__':
    main()
