import argparse
import json
import re
import sqlite3
import os

STUB_ITEMS = [
    {
        'id': 101,
        'title': 'The New Institutional Economics',
        'status': 'queued',
        'attempts': 0,
    },
    {
        'id': 102,
        'title': 'Markets and Hierarchies',
        'status': 'in_progress',
        'attempts': 1,
    },
]

ATTEMPT_RE = re.compile(r'Attempt\s+(\d+)', re.IGNORECASE)


def parse_attempts(status_notes):
    if not status_notes:
        return 0
    match = ATTEMPT_RE.search(status_notes)
    if match:
        return int(match.group(1))
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-path', required=True)
    parser.add_argument('--limit', type=int, default=200)
    parser.add_argument('--offset', type=int, default=0)
    args = parser.parse_args()

    if os.environ.get('RAG_FEEDER_STUB') == '1':
        print(json.dumps({'items': STUB_ITEMS, 'total': len(STUB_ITEMS), 'source': 'stub'}))
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    items = []
    try:
        rows = conn.execute("SELECT * FROM to_download_references").fetchall()
        for row in rows:
            data = dict(row)
            status_notes = data.get('status_notes')
            status = 'queued'
            if status_notes:
                lowered = status_notes.lower()
                if 'failed' in lowered or 'error' in lowered:
                    status = 'failed'
                elif 'processing' in lowered or 'downloading' in lowered:
                    status = 'in_progress'
            items.append({
                'id': data.get('id'),
                'title': data.get('title'),
                'status': status,
                'attempts': parse_attempts(status_notes),
            })
    except sqlite3.Error:
        items = []
    finally:
        conn.close()

    items.sort(key=lambda x: x.get('id') or 0, reverse=True)
    total = len(items)
    start = max(args.offset, 0)
    end = start + max(args.limit, 0)
    items = items[start:end]

    print(json.dumps({'items': items, 'total': total}))


if __name__ == '__main__':
    main()
