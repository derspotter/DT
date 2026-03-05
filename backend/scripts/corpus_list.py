import argparse
import json
import sqlite3
from pathlib import Path
from collections import Counter

STUB_ITEMS = [
    {
        'id': 1,
        'title': 'The New Institutional Economics',
        'year': 2002,
        'source': 'Seed bibliography',
        'status': 'with_metadata',
        'doi': '10.1017/cbo9780511613807.002',
        'openalex_id': 'W2175056322',
    },
    {
        'id': 2,
        'title': 'Markets and Hierarchies',
        'year': 1975,
        'source': 'Keyword search',
        'status': 'to_download_references',
        'doi': None,
        'openalex_id': 'W1974636593',
    },
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-path', required=True)
    parser.add_argument('--limit', type=int, default=200)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--corpus-id', type=int, default=None)
    args = parser.parse_args()

    if 'RAG_FEEDER_STUB' in __import__('os').environ:
        print(
            json.dumps(
                {
                    'items': STUB_ITEMS,
                    'total': len(STUB_ITEMS),
                    'source': 'stub',
                    'stage_totals': {
                        'raw': sum(1 for item in STUB_ITEMS if item.get('status') in ('no_metadata', 'extract_references_from_pdf', 'pending')),
                        'metadata': sum(1 for item in STUB_ITEMS if item.get('status') in ('with_metadata', 'to_download_references')),
                        'downloaded': sum(1 for item in STUB_ITEMS if item.get('status') == 'downloaded_references'),
                        'failed_enrichments': sum(1 for item in STUB_ITEMS if item.get('status') == 'failed_enrichments'),
                        'failed_downloads': sum(1 for item in STUB_ITEMS if item.get('status') == 'failed_downloads'),
                    },
                    'status_counts': dict(Counter(item.get('status') for item in STUB_ITEMS)),
                }
            )
        )
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    tables = [
        ('downloaded_references', 'downloaded_references'),
        ('with_metadata', 'with_metadata'),
        ('no_metadata', 'no_metadata'),
        ('failed_enrichments', 'failed_enrichments'),
        ('failed_downloads', 'failed_downloads'),
        ('to_download_references', 'to_download_references'),  # legacy fallback
    ]

    items = []
    for table, fallback_status in tables:
        try:
            if args.corpus_id is not None:
                rows = conn.execute(
                    f"""SELECT t.* FROM {table} t
                        JOIN corpus_items ci
                          ON ci.table_name = ? AND ci.row_id = t.id
                       WHERE ci.corpus_id = ?""",
                    (table, args.corpus_id),
                ).fetchall()
            else:
                rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        except sqlite3.Error:
            continue

        for row in rows:
            data = dict(row)
            status = fallback_status
            if table == 'with_metadata':
                state = (data.get('download_state') or '').strip().lower()
                if state in ('queued', 'in_progress'):
                    status = 'to_download_references'
                elif state == 'failed':
                    status = 'failed_downloads'
                else:
                    status = 'with_metadata'
            source = (
                data.get('ingest_source')
                or data.get('source_pdf')
                or data.get('metadata_source_type')
                or data.get('journal_conference')
                or data.get('source')
            )
            items.append(
                {
                    'id': data.get('id'),
                    'title': data.get('title'),
                    'year': data.get('year'),
                    'source': source,
                    'status': status,
                    'doi': data.get('doi'),
                    'openalex_id': data.get('openalex_id'),
                }
            )

    conn.close()

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

    status_counts = Counter(item.get('status') for item in items)
    stage_totals = {
        'raw': sum(status_counts.get(status, 0) for status in ('no_metadata', 'extract_references_from_pdf', 'pending')),
        'metadata': sum(status_counts.get(status, 0) for status in ('with_metadata', 'to_download_references')),
        'downloaded': status_counts.get('downloaded_references', 0),
        'failed_enrichments': status_counts.get('failed_enrichments', 0),
        'failed_downloads': status_counts.get('failed_downloads', 0),
    }

    items.sort(key=lambda x: (normalize_year(x.get('year')), normalize_id(x.get('id'))), reverse=True)
    total = len(items)
    start = max(args.offset, 0)
    end = start + max(args.limit, 0)
    items = items[start:end]

    print(
        json.dumps(
            {
                'items': items,
                'total': total,
                'stage_totals': stage_totals,
                'status_counts': dict(status_counts),
            }
        )
    )


if __name__ == '__main__':
    main()
