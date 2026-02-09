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
    parser.add_argument('--corpus-id', type=int, default=None)
    args = parser.parse_args()

    if os.environ.get('RAG_FEEDER_STUB') == '1':
        print(json.dumps({'items': STUB_ITEMS, 'total': len(STUB_ITEMS), 'source': 'stub'}))
        return

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row

    items = []
    try:
        if args.corpus_id is not None:
            rows = conn.execute(
                """SELECT t.* FROM to_download_references t
                   JOIN corpus_items ci
                     ON ci.table_name = 'to_download_references' AND ci.row_id = t.id
                  WHERE ci.corpus_id = ?""",
                (args.corpus_id,),
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM to_download_references").fetchall()
        for row in rows:
            data = dict(row)
            status_notes = data.get('status_notes')
            status = (data.get('download_state') or '').strip() or None
            # Back-compat for older DBs that don't have download_state yet.
            if not status:
                status = 'queued'
                if status_notes:
                    lowered = status_notes.lower()
                    if 'failed' in lowered or 'error' in lowered:
                        status = 'failed'
                    elif 'processing' in lowered or 'downloading' in lowered:
                        status = 'in_progress'

            attempts = data.get('download_attempt_count')
            if attempts is None:
                attempts = parse_attempts(status_notes)
            items.append({
                'id': data.get('id'),
                'title': data.get('title'),
                'status': status,
                'attempts': int(attempts or 0),
            })
    except sqlite3.Error:
        items = []
    finally:
        conn.close()

    status_rank = {'in_progress': 0, 'queued': 1, 'failed': 2}
    items.sort(key=lambda x: (status_rank.get(x.get('status') or '', 99), -int(x.get('id') or 0)))
    total = len(items)
    start = max(args.offset, 0)
    end = start + max(args.limit, 0)
    items = items[start:end]

    print(json.dumps({'items': items, 'total': total}))


if __name__ == '__main__':
    main()
