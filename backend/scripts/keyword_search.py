import argparse
import contextlib
import io
import json
import os
import re
import sys
from collections import deque
from pathlib import Path
from _bootstrap import ensure_import_paths

import requests

ROOT = Path(__file__).resolve().parents[2]
DL_LIT_PROJECT = ROOT / 'dl_lit_project'
ensure_import_paths(__file__)
if str(DL_LIT_PROJECT) not in sys.path:
    sys.path.insert(0, str(DL_LIT_PROJECT))

from dl_lit.OpenAlexScraper import fetch_referenced_work_details
from dl_lit.db_manager import DatabaseManager
from dl_lit.keyword_search import dedupe_results, search_openalex
from dl_lit.utils import get_global_rate_limiter

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

OPENALEX_WORKS_URL = 'https://api.openalex.org/works'
OPENALEX_SELECT = (
    'id,doi,display_name,authorships,publication_year,type,'
    'abstract_inverted_index,keywords,primary_location,open_access,biblio,'
    'referenced_works,cited_by_api_url'
)
OPENALEX_ID_RE = re.compile(r'^W\d+$', re.IGNORECASE)
DOI_RE = re.compile(r'10\.\d{4,9}/\S+', re.IGNORECASE)


def normalize_openalex_id(value):
    if not value:
        return None
    raw = str(value).strip()
    if raw.startswith('http'):
        raw = raw.rstrip('/').split('/')[-1]
    if OPENALEX_ID_RE.match(raw):
        return raw.upper()
    return None


def normalize_doi(value):
    if not value:
        return None
    raw = str(value).strip().lower()
    raw = raw.replace('https://doi.org/', '').replace('http://doi.org/', '')
    raw = raw.replace('doi:', '')
    match = DOI_RE.search(raw)
    return match.group(0) if match else None


def _openalex_get(params, mailto=None, retries=3):
    rate_limiter = get_global_rate_limiter()
    headers = {'Accept': 'application/json'}
    for attempt in range(retries):
        try:
            rate_limiter.wait_if_needed('openalex')
            request_params = dict(params)
            if mailto:
                request_params['mailto'] = mailto
            response = requests.get(OPENALEX_WORKS_URL, headers=headers, params=request_params, timeout=30)
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == retries - 1:
                raise
    return {}


def fetch_work_by_openalex_id(openalex_id, mailto=None):
    work_id = normalize_openalex_id(openalex_id)
    if not work_id:
        return None
    data = _openalex_get({'filter': f'openalex_id:{work_id}', 'per-page': 1, 'select': OPENALEX_SELECT}, mailto=mailto)
    results = data.get('results') or []
    return results[0] if results else None


def fetch_work_by_doi(doi, mailto=None):
    norm = normalize_doi(doi)
    if not norm:
        return None
    data = _openalex_get({'filter': f'doi:{norm}', 'per-page': 1, 'select': OPENALEX_SELECT}, mailto=mailto)
    results = data.get('results') or []
    return results[0] if results else None


def _to_openalex_like(work):
    if not isinstance(work, dict):
        return None

    item_id = work.get('id') or work.get('openalex_id')
    item_id = normalize_openalex_id(item_id) or item_id
    if not item_id:
        return None
    if not str(item_id).startswith('http'):
        item_id = f'https://openalex.org/{item_id}'

    if work.get('authorships'):
        authorships = work.get('authorships')
    else:
        authorships = [
            {'author': {'display_name': name}}
            for name in (work.get('authors') or [])
            if isinstance(name, str) and name.strip()
        ]

    display_name = work.get('display_name') or work.get('title')
    out = {
        'id': item_id,
        'doi': work.get('doi'),
        'display_name': display_name,
        'authorships': authorships,
        'publication_year': work.get('publication_year') or work.get('year'),
        'type': work.get('type'),
        'abstract_inverted_index': work.get('abstract_inverted_index') or work.get('abstract'),
        'keywords': work.get('keywords') or [],
        'primary_location': work.get('primary_location') or {'source': {'display_name': work.get('source')}},
        'open_access': work.get('open_access') or {},
        'biblio': work.get('biblio') or {},
        'referenced_works': work.get('referenced_works') or [],
        'cited_by_api_url': work.get('cited_by_api_url'),
    }
    return out


def openalex_result_to_record(item, run_id=None):
    biblio = item.get('biblio') or {}
    primary_location = item.get('primary_location') or {}
    source_info = primary_location.get('source') or {}
    authors = [a.get('author', {}).get('display_name') for a in item.get('authorships', [])]

    first_page = biblio.get('first_page')
    last_page = biblio.get('last_page')
    pages = f'{first_page}--{last_page}' if first_page and last_page else None

    return {
        'openalex_id': item.get('id'),
        'doi': item.get('doi'),
        'title': item.get('display_name'),
        'year': item.get('publication_year'),
        'authors': authors,
        'abstract': item.get('abstract_inverted_index'),
        'keywords': [kw.get('display_name') for kw in item.get('keywords', []) if kw.get('display_name')],
        'source': source_info.get('display_name'),
        'volume': biblio.get('volume'),
        'issue': biblio.get('issue'),
        'pages': pages,
        'publisher': source_info.get('publisher'),
        'type': item.get('type'),
        'url': item.get('id'),
        'open_access_url': item.get('open_access', {}).get('oa_url'),
        'openalex_json': item,
        'ingest_source': 'keyword_search',
        'run_id': run_id,
    }


def _dedupe_openalex_items(items):
    seen = set()
    deduped = []
    for item in items:
        openalex_like = _to_openalex_like(item)
        if not openalex_like:
            continue
        key = openalex_like.get('id')
        if key in seen:
            continue
        seen.add(key)
        deduped.append(openalex_like)
    return deduped


def _extract_ref_ids(work):
    refs = work.get('referenced_works') or []
    out = []
    for ref in refs:
        if isinstance(ref, str):
            out.append(ref)
        elif isinstance(ref, dict):
            out.append(ref.get('id') or ref.get('openalex_id'))
    return [rid for rid in out if rid]


def expand_references_recursive(base_items, related_depth, max_related, mailto):
    if related_depth <= 1 or not base_items:
        return base_items, {'added': 0, 'processed': 0}

    queue = deque()
    all_items = _dedupe_openalex_items(base_items)
    seen = {item.get('id') for item in all_items if item.get('id')}
    processed = 0
    added = 0

    for item in all_items:
        queue.append((item, 1))

    rate_limiter = get_global_rate_limiter()

    while queue:
        current, level = queue.popleft()
        processed += 1
        if level >= related_depth:
            continue

        ref_ids = _extract_ref_ids(current)
        if not ref_ids and current.get('id'):
            refreshed = fetch_work_by_openalex_id(current.get('id'), mailto=mailto)
            if refreshed:
                current = _to_openalex_like(refreshed) or current
                ref_ids = _extract_ref_ids(current)

        if max_related and max_related > 0:
            ref_ids = ref_ids[:max_related]
        if not ref_ids:
            continue

        ref_details = fetch_referenced_work_details(
            ref_ids,
            rate_limiter,
            mailto=mailto or 'spott@wzb.eu',
            include_links=(level + 1 < related_depth),
        )

        for ref in ref_details:
            item = _to_openalex_like(ref)
            if not item:
                continue
            key = item.get('id')
            if key in seen:
                continue
            seen.add(key)
            all_items.append(item)
            queue.append((item, level + 1))
            added += 1

    return all_items, {'added': added, 'processed': processed}


def _parse_seed_json(seed_json):
    if not seed_json:
        return []
    candidate = Path(seed_json)
    if candidate.exists() and candidate.is_file():
        data = json.loads(candidate.read_text(encoding='utf-8'))
    else:
        data = json.loads(seed_json)
    if isinstance(data, dict):
        if isinstance(data.get('seeds'), list):
            data = data['seeds']
        elif isinstance(data.get('items'), list):
            data = data['items']
        else:
            data = [data]
    if not isinstance(data, list):
        raise ValueError('seed_json must resolve to a JSON list or object with seeds/items.')
    return data


def _resolve_seed_item(seed_item, mailto=None):
    if isinstance(seed_item, dict):
        openalex_id = seed_item.get('openalex_id') or seed_item.get('id')
        doi = seed_item.get('doi')
        title = seed_item.get('title') or seed_item.get('query')
    else:
        token = str(seed_item).strip()
        openalex_id = token
        doi = token
        title = token

    work = fetch_work_by_openalex_id(openalex_id, mailto=mailto)
    if work:
        return work

    work = fetch_work_by_doi(doi, mailto=mailto)
    if work:
        return work

    if title:
        results = search_openalex(query=title, max_results=1, mailto=mailto, field='default')
        if results:
            return results[0]
    return None


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
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--query')
    mode.add_argument('--seed-json', default=None)
    parser.add_argument('--db-path', required=True)
    parser.add_argument('--max-results', type=int, default=200)
    parser.add_argument('--year-from', type=int, default=None)
    parser.add_argument('--year-to', type=int, default=None)
    parser.add_argument('--field', default='default')
    parser.add_argument('--mailto', default=None)
    parser.add_argument('--enqueue', action='store_true')
    parser.add_argument('--corpus-id', type=int, default=None)
    parser.add_argument('--related-depth', type=int, default=1)
    parser.add_argument('--max-related', type=int, default=30)
    args = parser.parse_args()

    if os.environ.get('RAG_FEEDER_STUB') == '1':
        print(json.dumps({'runId': 0, 'results': STUB_RESULTS, 'source': 'stub'}))
        return

    db_path = Path(args.db_path)
    related_depth = max(1, int(args.related_depth or 1))
    max_related = max(1, int(args.max_related or 1))

    with contextlib.redirect_stdout(io.StringIO()):
        db = DatabaseManager(db_path=db_path)
        mode_label = 'seed' if args.seed_json else 'query'
        filters = {
            'mode': mode_label,
            'max_results': args.max_results,
            'year_from': args.year_from,
            'year_to': args.year_to,
            'field': args.field,
            'mailto': args.mailto,
            'related_depth': related_depth,
            'max_related': max_related,
        }
        run_query_label = args.query if args.query else '[seed-json]'
        run_id = db.create_search_run(query=run_query_label, filters=filters)

        if args.query:
            base_items = search_openalex(
                query=args.query,
                max_results=args.max_results,
                year_from=args.year_from,
                year_to=args.year_to,
                field=args.field,
                mailto=args.mailto,
            )
        else:
            seeds = _parse_seed_json(args.seed_json)
            base_items = []
            for seed in seeds:
                work = _resolve_seed_item(seed, mailto=args.mailto)
                if work:
                    base_items.append(work)

        all_items, expansion_stats = expand_references_recursive(
            base_items,
            related_depth=related_depth,
            max_related=max_related,
            mailto=args.mailto,
        )

        records = [openalex_result_to_record(item, run_id=run_id) for item in _dedupe_openalex_items(all_items)]
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
        'mode': mode_label,
        'expansion': expansion_stats,
    }
    print(json.dumps(payload))


if __name__ == '__main__':
    main()
