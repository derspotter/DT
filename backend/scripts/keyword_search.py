import argparse
import contextlib
import io
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DL_LIT_PROJECT = ROOT / 'dl_lit_project'
if str(DL_LIT_PROJECT) not in sys.path:
    sys.path.insert(0, str(DL_LIT_PROJECT))

from dl_lit.db_manager import DatabaseManager
from dl_lit.keyword_search import search_openalex, openalex_result_to_record, dedupe_results

STUB_RESULTS = [
    {
        'id': 'W2175056322',
        'openalex_id': 'W2175056322',
        'title': 'The New Institutional Economics',
        'authors': 'Ronald H. Coase',
        'year': 2002,
        'type': 'book-chapter',
        'doi': '10.1017/cbo9780511613807.002',
    },
    {
        'id': 'W2015930340',
        'openalex_id': 'W2015930340',
        'title': 'The Nature of the Firm',
        'authors': 'Ronald H. Coase',
        'year': 1937,
        'type': 'journal-article',
        'doi': '10.1111/j.1468-0335.1937.tb00002.x',
    },
]


def _render_results(records):
    results = []
    for record in records:
        authors = record.get('authors') or []
        results.append({
            'id': record.get('openalex_id') or record.get('doi') or record.get('title'),
            'openalex_id': record.get('openalex_id'),
            'doi': record.get('doi'),
            'title': record.get('title'),
            'year': record.get('year'),
            'authors': ', '.join(authors) if isinstance(authors, list) else authors,
            'type': record.get('type'),
        })
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', required=True)
    parser.add_argument('--db-path', required=True)
    parser.add_argument('--max-results', type=int, default=200)
    parser.add_argument('--year-from', type=int, default=None)
    parser.add_argument('--year-to', type=int, default=None)
    parser.add_argument('--field', default='default')
    parser.add_argument('--mailto', default=None)
    parser.add_argument('--enqueue', action='store_true')
    parser.add_argument('--corpus-id', type=int, default=None)
    args = parser.parse_args()

    if os.environ.get('RAG_FEEDER_STUB') == '1':
        print(json.dumps({'runId': 0, 'results': STUB_RESULTS, 'source': 'stub'}))
        return

    db_path = Path(args.db_path)

    with contextlib.redirect_stdout(io.StringIO()):
        db = DatabaseManager(db_path=db_path)
        run_id = db.create_search_run(query=args.query, filters={
            'max_results': args.max_results,
            'year_from': args.year_from,
            'year_to': args.year_to,
            'field': args.field,
            'mailto': args.mailto,
        })

        raw_results = search_openalex(
            query=args.query,
            max_results=args.max_results,
            year_from=args.year_from,
            year_to=args.year_to,
            field=args.field,
            mailto=args.mailto,
        )

        records = [openalex_result_to_record(item, run_id=run_id) for item in raw_results]
        records = dedupe_results(records)

        db.add_search_results(run_id, [
            {
                'openalex_id': r.get('openalex_id'),
                'doi': r.get('doi'),
                'title': r.get('title'),
                'year': r.get('year'),
                'raw_json': r.get('openalex_json'),
            }
            for r in records
        ])

        if args.enqueue:
            for record in records:
                db.add_entry_to_download_queue(record, corpus_id=args.corpus_id)

        db.close_connection()

    payload = {
        'runId': run_id,
        'results': _render_results(records),
        'source': 'openalex',
    }
    print(json.dumps(payload))


if __name__ == '__main__':
    main()
