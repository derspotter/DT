import argparse
import hashlib
import json
import os
import re
import sqlite3
import time

STUB_GRAPH = {
    'nodes': [
        {
            'id': 'W2175056322',
            'title': 'The New Institutional Economics',
            'year': 2002,
            'type': 'book-chapter',
            'status': 'with_metadata',
            'source_path': 'seed_pdf',
            'ingest_source': 'seed_pdf',
        },
        {
            'id': 'W2015930340',
            'title': 'The Nature of the Firm',
            'year': 1937,
            'type': 'journal-article',
            'status': 'with_metadata',
            'source_path': 'keyword_search',
            'ingest_source': 'keyword_search',
        },
    ],
    'edges': [
        {'source': 'W2175056322', 'target': 'W2015930340', 'relationship_type': 'references'},
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


def derive_source_path_label(ingest_source, source_pdf, run_id):
    ingest = str(ingest_source or '').strip()
    if ingest:
        return ingest
    source_pdf_value = str(source_pdf or '').strip()
    if source_pdf_value:
        return os.path.basename(source_pdf_value)
    if run_id is not None:
        return f'run:{run_id}'
    return 'unknown'


def ensure_graph_cache_table(conn):
    conn.executescript(
        '''
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
        '''
    )


def build_cache_key(args):
    payload = {
        'max_nodes': args.max_nodes,
        'relationship': args.relationship,
        'status': args.status,
        'year_from': args.year_from,
        'year_to': args.year_to,
        'corpus_id': args.corpus_id,
        'run_id': args.run_id,
        'source': args.source,
        'hide_isolates': args.hide_isolates,
    }
    normalized = {
        key: stable_signature(value)
        for key, value in payload.items()
        if value is not None and value != ''
    }
    signature = json.dumps(normalized, sort_keys=True)
    return hashlib.sha256(signature.encode('utf-8')).hexdigest(), signature


def try_read_cache(conn, cache_key, ttl_minutes):
    if ttl_minutes <= 0:
        return None
    now = int(time.time())
    row = conn.execute(
        'SELECT payload_json, created_at FROM graph_exports WHERE cache_key = ?',
        (cache_key,),
    ).fetchone()
    if not row:
        return None
    created_at = int(row['created_at'] or 0)
    ttl_seconds = int(ttl_minutes) * 60
    if created_at + ttl_seconds < now:
        conn.execute('DELETE FROM graph_exports WHERE cache_key = ?', (cache_key,))
        conn.commit()
        return None
    try:
        return json.loads(row['payload_json'])
    except (TypeError, json.JSONDecodeError):
        return None


def write_cache(conn, cache_key, signature, payload):
    now = int(time.time())
    payload_json = json.dumps(payload)
    payload_stats = json.dumps(payload.get('stats', {}))
    conn.execute(
        '''
        INSERT INTO graph_exports (
            cache_key,
            request_signature,
            payload_json,
            payload_stats_json,
            nodes,
            edges,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(cache_key)
        DO UPDATE SET
            request_signature = excluded.request_signature,
            payload_json = excluded.payload_json,
            payload_stats_json = excluded.payload_stats_json,
            nodes = excluded.nodes,
            edges = excluded.edges,
            created_at = excluded.created_at
        ''',
        (
            cache_key,
            signature,
            payload_json,
            payload_stats,
            int(payload.get('stats', {}).get('node_count', 0)),
            int(payload.get('stats', {}).get('edge_count', 0)),
            now,
        ),
    )
    conn.commit()


def propagate_source_paths(nodes, edges):
    node_map = {node.get('id'): node for node in nodes if node.get('id')}
    adjacency = {}
    for edge in edges:
        source_id = edge.get('source')
        target_id = edge.get('target')
        if not source_id or not target_id:
            continue
        adjacency.setdefault(source_id, set()).add(target_id)
        adjacency.setdefault(target_id, set()).add(source_id)

    for node in nodes:
        if node.get('source_path') and node.get('source_path') != 'unknown':
            continue
        node_id = node.get('id')
        if not node_id:
            continue
        candidates = set()
        for neighbor_id in adjacency.get(node_id, set()):
            neighbor = node_map.get(neighbor_id)
            if not neighbor:
                continue
            label = neighbor.get('source_path')
            if label and label != 'unknown':
                candidates.add(label)
        if len(candidates) == 1:
            node['source_path'] = next(iter(candidates))


def merge_status(existing, incoming):
    if not existing:
        return incoming
    if isinstance(existing, list):
        if incoming in existing:
            return existing
        return sorted(set(existing + [incoming]))
    if existing == incoming:
        return existing
    return sorted({existing, incoming})


def node_status_from_row(table_name, row_data):
    if table_name == 'with_metadata':
        state = (row_data.get('download_state') or '').strip().lower()
        if state in ('queued', 'in_progress'):
            return 'queued'
        return 'with_metadata'
    if table_name == 'to_download_references':
        return 'to_download_references'
    if table_name == 'downloaded_references':
        return 'downloaded'
    return table_name


def normalize_node_key(row_data):
    return (
        normalize_openalex_id(row_data.get('openalex_id'))
        or normalize_doi(row_data.get('doi'))
        or f"{row_data.get('_table')}:{row_data.get('id')}"
    )


def build_node(row_data):
    return {
        'id': row_data['id'],
        'title': row_data.get('title'),
        'year': row_data.get('year'),
        'type': row_data.get('entry_type') or row_data.get('type'),
        'status': row_data.get('_status'),
        'doi': row_data.get('doi'),
        'openalex_id': normalize_openalex_id(row_data.get('openalex_id')) or row_data.get('openalex_id'),
        'ingest_source': row_data.get('ingest_source'),
        'source_pdf': row_data.get('source_pdf'),
        'source_path': row_data.get('_source_path'),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-path', required=True)
    parser.add_argument('--max-nodes', type=int, default=200)
    parser.add_argument('--relationship', choices=['references', 'cited_by', 'both'], default='both')
    parser.add_argument(
        '--status',
        choices=['downloaded', 'queued', 'with_metadata', 'to_download_references', 'all'],
        default='all',
    )
    parser.add_argument('--year-from', type=int, default=None)
    parser.add_argument('--year-to', type=int, default=None)
    parser.add_argument('--hide-isolates', type=int, default=1)
    parser.add_argument('--corpus-id', type=int, default=None)
    parser.add_argument('--run-id', type=int, default=None)
    parser.add_argument('--source', type=str, default=None)
    parser.add_argument('--cache-ttl-minutes', type=int, default=15)
    parser.add_argument('--force-refresh', action='store_true')
    args = parser.parse_args()

    if os.environ.get('RAG_FEEDER_STUB') == '1':
        print(json.dumps({**STUB_GRAPH, 'source': 'stub'}))
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    ensure_graph_cache_table(conn)

    if args.hide_isolates not in (0, 1):
        args.hide_isolates = 1 if str(args.hide_isolates).strip() == '1' else 0

    cache_key, signature = build_cache_key(args)
    if not args.force_refresh:
        cached = try_read_cache(
            conn,
            cache_key,
            max(0, int(args.cache_ttl_minutes) if args.cache_ttl_minutes is not None else 15),
        )
        if cached is not None:
            cached['source'] = 'cache'
            conn.close()
            print(json.dumps(cached))
            return

    def has_table(name: str) -> bool:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None

    def has_column(table_name: str, column_name: str) -> bool:
        if not has_table(table_name):
            return False
        for col in conn.execute(f'PRAGMA table_info({table_name})').fetchall():
            if col[1] == column_name:
                return True
        return False

    def include_year(data):
        year = data.get('year')
        if args.year_from is not None and (year is None or year < args.year_from):
            return False
        if args.year_to is not None and (year is None or year > args.year_to):
            return False
        return True

    def include_source(data):
        if not args.source:
            return True
        source = str(data.get('ingest_source') or '').strip().lower()
        return source == str(args.source).strip().lower()

    def include_run(data):
        if args.run_id is None:
            return True
        run_id = data.get('run_id')
        try:
            return run_id is not None and int(run_id) == int(args.run_id)
        except (TypeError, ValueError):
            return False

    def node_status_ok(table_name, data):
        status = node_status_from_row(table_name, data)
        if args.status == 'all':
            return True
        return status == args.status

    def with_metadata_state_allowed(row_data):
        if args.status == 'queued':
            state = (row_data.get('download_state') or '').strip().lower()
            return state in ('queued', 'in_progress')
        if args.status == 'with_metadata':
            state = (row_data.get('download_state') or '').strip().lower()
            return state not in ('queued', 'in_progress')
        return True

    allowed_rows = None
    if args.corpus_id is not None:
        try:
            rows = conn.execute(
                "SELECT table_name, row_id FROM corpus_items WHERE corpus_id = ?",
                (args.corpus_id,),
            ).fetchall()
            allowed_rows = {}
            for row in rows:
                allowed_rows.setdefault(row['table_name'], set()).add(row['row_id'])
        except sqlite3.Error:
            allowed_rows = None

    tables = []
    if args.status in ('all', 'queued', 'with_metadata'):
        tables.append(('with_metadata', node_status_from_row))
    if args.status in ('all', 'to_download_references'):
        tables.append(('to_download_references', node_status_from_row))
    if args.status in ('all', 'downloaded'):
        tables.append(('downloaded_references', node_status_from_row))

    node_by_id = {}
    row_candidates = []
    rows_by_table = {table: [] for table, _ in tables}
    node_candidates = []

    for table_name, _status_fn in tables:
        if not has_table(table_name):
            continue

        for row in conn.execute(f'SELECT * FROM {table_name}').fetchall():
            data = dict(row)
            data['_table'] = table_name

            if allowed_rows is not None:
                allowed_ids = allowed_rows.get(table_name)
                if allowed_ids is not None and data.get('id') not in allowed_ids:
                    continue

            if not include_year(data):
                continue
            if not include_source(data):
                continue
            if not include_run(data):
                continue
            if table_name == 'with_metadata' and not with_metadata_state_allowed(data):
                continue
            status = node_status_from_row(table_name, data)
            if not node_status_ok(table_name, data):
                continue

            node_id = normalize_node_key(data)
            if not node_id:
                continue

            data['_status'] = status
            data['_source_path'] = derive_source_path_label(data.get('ingest_source'), data.get('source_pdf'), data.get('run_id'))
            data['_node_id'] = node_id
            rows_by_table[table_name].append(data)

            row_candidates.append((table_name, data))
            existing = node_by_id.get(node_id)
            if existing is None:
                node_by_id[node_id] = {
                    'id': node_id,
                    'title': data.get('title'),
                    'year': data.get('year'),
                    'type': data.get('entry_type') or data.get('type'),
                    'status': status,
                    'doi': data.get('doi'),
                    'openalex_id': normalize_openalex_id(data.get('openalex_id')) or data.get('openalex_id'),
                    'ingest_source': data.get('ingest_source'),
                    'source_pdf': data.get('source_pdf'),
                    'source_path': data['_source_path'],
                }
            else:
                existing['status'] = merge_status(existing.get('status'), status)

            node_candidates.append(node_id)

    if not node_by_id:
        print(
            json.dumps(
                {
                    'nodes': [],
                    'edges': [],
                    'stats': {
                        'node_count': 0,
                        'edge_count': 0,
                        'relationship_counts': {'references': 0, 'cited_by': 0},
                        'cached': False,
                    },
                }
            )
        )
        conn.close()
        return

    allowed_node_ids = set(node_by_id.keys())
    edges = []
    used_edge_table = False

    if has_table('citation_edges'):
        edge_query = 'SELECT source_id, target_id, relationship_type, run_id FROM citation_edges'
        if has_column('citation_edges', 'run_id'):
            edge_query = 'SELECT source_id, target_id, relationship_type, run_id FROM citation_edges'

        try:
            raw_edges = conn.execute(edge_query).fetchall()
        except sqlite3.Error:
            raw_edges = []

        filtered_edges = []
        for row in raw_edges:
            source_id = row['source_id']
            target_id = row['target_id']
            relationship = row['relationship_type']
            if source_id == target_id:
                continue
            if args.relationship != 'both' and relationship != args.relationship:
                continue
            if source_id not in allowed_node_ids or target_id not in allowed_node_ids:
                continue
            if args.run_id is not None and row['run_id'] is not None:
                try:
                    if int(row['run_id']) != int(args.run_id):
                        continue
                except (TypeError, ValueError):
                    continue
            filtered_edges.append((source_id, target_id, relationship))

        if filtered_edges:
            degree = {}
            adjacency = {}
            for source_id, target_id, relationship in filtered_edges:
                adjacency.setdefault(source_id, set()).add(target_id)
                adjacency.setdefault(target_id, set()).add(source_id)
                degree[source_id] = degree.get(source_id, 0) + 1
                degree[target_id] = degree.get(target_id, 0) + 1

            top_nodes = sorted(degree.items(), key=lambda item: item[1], reverse=True)
            seed_count = max(5, min(50, max(1, args.max_nodes // 10)))
            wanted_ids = set()
            queue = []
            for node_id, _ in top_nodes[:seed_count]:
                if node_id in wanted_ids:
                    continue
                wanted_ids.add(node_id)
                queue.append(node_id)

            index = 0
            while index < len(queue) and len(wanted_ids) < args.max_nodes:
                current = queue[index]
                index += 1
                for neighbor in adjacency.get(current, ()): 
                    if neighbor in wanted_ids:
                        continue
                    wanted_ids.add(neighbor)
                    queue.append(neighbor)
                    if len(wanted_ids) >= args.max_nodes:
                        break

            edges = [
                {'source': source_id, 'target': target_id, 'relationship_type': relationship}
                for source_id, target_id, relationship in filtered_edges
                if source_id in wanted_ids and target_id in wanted_ids
            ]
            if edges:
                used_edge_table = True

    if not used_edge_table:
        source_lookup = {}
        openalex_lookup = {}
        doi_lookup = {}

        for row_tbl, row in row_candidates:
            row_node_id = row.get('_node_id')
            if not row_node_id:
                continue
            row_id = row.get('id')
            if row_id is not None:
                source_lookup[(row_tbl, str(row_id))] = row_node_id

            openalex_id = normalize_openalex_id(row.get('openalex_id'))
            if openalex_id:
                source_lookup[(row_tbl, openalex_id)] = row_node_id
                openalex_lookup[openalex_id] = row_node_id

            doi = normalize_doi(row.get('doi'))
            if doi:
                source_lookup[(row_tbl, doi)] = row_node_id
                doi_lookup[doi] = row_node_id

        edge_set = set()
        for table_name, data in row_candidates:
            source_work_id = data.get('source_work_id')
            if not source_work_id:
                continue

            source_node = node_by_id.get(data['_node_id'])
            if source_node is None:
                continue

            target_node_id = data['_node_id']
            source_candidate = None

            candidates = [
                str(source_work_id).strip(),
            ]
            source_work_id_openalex = normalize_openalex_id(source_work_id)
            if source_work_id_openalex:
                candidates.append(source_work_id_openalex)
            source_work_id_doi = normalize_doi(source_work_id)
            if source_work_id_doi:
                candidates.append(source_work_id_doi)

            for candidate in candidates:
                if not candidate:
                    continue
                source_candidate = source_lookup.get((table_name, candidate))
                if source_candidate:
                    break
                source_candidate = openalex_lookup.get(candidate)
                if source_candidate:
                    break
                source_candidate = doi_lookup.get(candidate)
                if source_candidate:
                    break

            if source_candidate is None:
                source_candidate = next(
                    (
                        row.get('_node_id')
                        for row_tbl, row in row_candidates
                        if row.get('id') == source_work_id
                    ),
                    None,
                )

            if not source_candidate:
                continue

            relationship_type = data.get('relationship_type') or 'references'
            if args.relationship != 'both' and relationship_type != args.relationship:
                continue

            edge_key = (source_candidate, target_node_id, relationship_type)
            if edge_key in edge_set:
                continue
            edge_set.add(edge_key)
            edges.append({
                'source': source_candidate,
                'target': target_node_id,
                'relationship_type': relationship_type,
            })

    nodes = list(node_by_id.values())

    # Apply node limit only as a fallback when edges are not available (or when graph is too dense).
    if used_edge_table and len(nodes) > args.max_nodes:
        node_ids = {edge['source'] for edge in edges} | {edge['target'] for edge in edges}
        nodes = [node_by_id[nid] for nid in node_ids if nid in node_by_id]
        nodes = sorted(nodes, key=lambda item: item.get('id'))
        if len(nodes) > args.max_nodes:
            nodes = nodes[: args.max_nodes]
            wanted = {node['id'] for node in nodes}
            edges = [
                edge for edge in edges
                if edge['source'] in wanted and edge['target'] in wanted
            ]

    if len(edges) > 0 and args.max_nodes > 0:
        if nodes and max(item.get('id') for item in nodes) and args.max_nodes > 0:
            if len(nodes) > args.max_nodes:
                node_ids = {item['id'] for item in nodes[:args.max_nodes]}
                edges = [
                    edge for edge in edges
                    if edge['source'] in node_ids and edge['target'] in node_ids
                ]

    if not edges:
        # Ensure no stale source/target lookups when fallback edges were not generated.
        node_ids = {node['id'] for node in nodes}
        edges = [edge for edge in edges if edge['source'] in node_ids and edge['target'] in node_ids]

    propagate_source_paths(nodes, edges)

    degree = {}
    for edge in edges:
        degree[edge['source']] = degree.get(edge['source'], 0) + 1
        degree[edge['target']] = degree.get(edge['target'], 0) + 1

    if args.hide_isolates:
        nodes = [node for node in nodes if degree.get(node['id'], 0) > 0]
        node_ids = {node['id'] for node in nodes}
        edges = [
            edge for edge in edges
            if edge['source'] in node_ids and edge['target'] in node_ids
        ]
        degree = {}
        for edge in edges:
            degree[edge['source']] = degree.get(edge['source'], 0) + 1
            degree[edge['target']] = degree.get(edge['target'], 0) + 1

    for node in nodes:
        node['degree'] = degree.get(node['id'], 0)

    payload = {
        'nodes': nodes,
        'edges': edges,
        'stats': {
            'node_count': len(nodes),
            'edge_count': len(edges),
            'relationship_counts': {
                'references': sum(1 for edge in edges if edge.get('relationship_type') == 'references'),
                'cited_by': sum(1 for edge in edges if edge.get('relationship_type') == 'cited_by'),
            },
        },
    }

    write_cache(conn, cache_key, signature, payload)
    print(json.dumps(payload))
    conn.close()


if __name__ == '__main__':
    main()
