"""Materialise a promotion's expansion as NEW reviewable seeds.

Promotion mode "new seed" (jochen_enriched.md lines 10 & 11): instead of pulling
every related work straight into the corpus, collect them and land them in
section 2 as fresh seeds — one "Downstream of «title»" seed and one
"Upstream of «title»" seed per promoted work — so a human decides what belongs.

Related works are ranked before the max-related cap is applied (line 12), reusing
the same helpers keyword_search.py uses so both paths agree on what "top N" means.

Emits JSON: {"runs": [{"run_id": int, "direction": str, "label": str, "count": int}]}
"""

import argparse
import json
import sys
from pathlib import Path

from _bootstrap import ensure_import_paths

ROOT = Path(__file__).resolve().parents[2]
DL_LIT_PROJECT = ROOT / 'dl_lit_project'
ensure_import_paths(__file__)
if str(DL_LIT_PROJECT) not in sys.path:
    sys.path.insert(0, str(DL_LIT_PROJECT))

from dl_lit.OpenAlexScraper import fetch_referenced_work_details
from dl_lit.db_manager import DatabaseManager
from dl_lit.utils import OpenAlexRateLimitExceeded, get_global_rate_limiter

from keyword_search import (
    DEFAULT_RELATED_SORT,
    RELATED_SORT_OPTIONS,
    _collect_candidate_ids,
    _normalize_candidate_id,
    _to_openalex_like,
    fetch_work_by_doi,
    fetch_work_by_openalex_id,
    openalex_result_to_record,
)

# Keep seed labels readable in the section 2 list.
_LABEL_TITLE_LIMIT = 80


def _shorten(title, limit=_LABEL_TITLE_LIMIT):
    text = str(title or '').strip() or 'untitled work'
    if len(text) <= limit:
        return text
    return f'{text[: limit - 1].rstrip()}…'


def _resolve_work(seed, mailto):
    """Resolve a promoted item to a full OpenAlex work.

    referenced_works / cited_by_api_url are needed to expand from it, and the
    promotion payload does not always carry them.
    """
    openalex_id = seed.get('openalex_id') or seed.get('id')
    if openalex_id:
        work = fetch_work_by_openalex_id(openalex_id, mailto=mailto)
        if work:
            return _to_openalex_like(work)
    doi = seed.get('doi')
    if doi:
        work = fetch_work_by_doi(doi, mailto=mailto)
        if work:
            return _to_openalex_like(work)
    return None


def _collect_for_direction(work, direction, max_related, related_sort, mailto, rate_limiter):
    downstream_ids, upstream_ids = _collect_candidate_ids(
        work,
        max_related=max_related,
        rate_limiter=rate_limiter,
        mailto=mailto,
        direction=direction,
        related_sort=related_sort,
    )
    return downstream_ids if direction == 'downstream' else upstream_ids


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-path', required=True)
    parser.add_argument('--corpus-id', type=int, default=None)
    seed_source = parser.add_mutually_exclusive_group(required=True)
    seed_source.add_argument('--seed-json', help='JSON list of promoted items')
    # A large promotion's seed list can exceed ARG_MAX on the command line, so
    # the backend writes it to a temp file instead (same as seed_promote.py).
    seed_source.add_argument('--seed-file', help='Path to a JSON file with the promoted items')
    parser.add_argument('--max-related', type=int, default=30)
    parser.add_argument('--related-sort', default=DEFAULT_RELATED_SORT, choices=sorted(RELATED_SORT_OPTIONS.keys()))
    parser.add_argument('--include-downstream', action='store_true', default=False)
    parser.add_argument('--include-upstream', action='store_true', default=False)
    parser.add_argument('--mailto', default=None)
    args = parser.parse_args()

    try:
        if args.seed_file:
            seeds = json.loads(Path(args.seed_file).read_text(encoding='utf-8'))
        else:
            seeds = json.loads(args.seed_json)
    except (json.JSONDecodeError, OSError) as exc:
        print(json.dumps({'runs': [], 'error': f'Invalid seed JSON: {exc}'}))
        return
    if not isinstance(seeds, list):
        seeds = []

    directions = []
    if args.include_downstream:
        directions.append('downstream')
    if args.include_upstream:
        directions.append('upstream')
    if not directions or not seeds:
        print(json.dumps({'runs': []}))
        return

    rate_limiter = get_global_rate_limiter()
    db = DatabaseManager(db_path=args.db_path)
    runs = []

    try:
        for seed in seeds:
            if not isinstance(seed, dict):
                continue
            work = _resolve_work(seed, args.mailto)
            if not work:
                continue
            title = seed.get('title') or work.get('display_name')

            for direction in directions:
                try:
                    candidate_ids = _collect_for_direction(
                        work, direction, args.max_related, args.related_sort, args.mailto, rate_limiter
                    )
                except OpenAlexRateLimitExceeded as exc:
                    print(f'[OpenAlex WARN] {direction} expansion stopped early: {exc}', file=sys.stderr)
                    continue
                if not candidate_ids:
                    continue

                try:
                    details = fetch_referenced_work_details(
                        candidate_ids,
                        rate_limiter,
                        mailto=args.mailto or 'spott@wzb.eu',
                        include_links=False,
                    )
                except OpenAlexRateLimitExceeded as exc:
                    print(f'[OpenAlex WARN] {direction} detail fetch stopped early: {exc}', file=sys.stderr)
                    continue

                items = []
                seen = set()
                for ref in details:
                    item = _to_openalex_like(ref)
                    if not item:
                        continue
                    norm = _normalize_candidate_id(item.get('id'))
                    if not norm or norm in seen:
                        continue
                    seen.add(norm)
                    items.append(item)
                if not items:
                    continue

                label = f'{direction.capitalize()} of «{_shorten(title)}»'
                run_id = db.create_search_run(
                    query=label,
                    filters={
                        'expansion_direction': direction,
                        'expansion_of_openalex_id': work.get('id'),
                        'expansion_of_title': title,
                        'max_related': args.max_related,
                        'related_sort': args.related_sort,
                    },
                )
                records = [openalex_result_to_record(item, run_id=run_id) for item in items]
                db.add_search_results(run_id, [
                    {
                        'openalex_id': record.get('openalex_id'),
                        'doi': record.get('doi'),
                        'title': record.get('title'),
                        'year': record.get('year'),
                        'raw_json': record.get('openalex_json'),
                    }
                    for record in records
                ])
                runs.append({
                    'run_id': run_id,
                    'direction': direction,
                    'label': label,
                    'count': len(records),
                })
    finally:
        db.close_connection()

    print(json.dumps({'runs': runs}))


if __name__ == '__main__':
    main()
