"""Explicit, bounded repair of empty outputs, never a corpus import or embed.

Run inside corpus-updater with a saved DT manifest and --expected-count.
Outputs/backups are on the persistent corpus volume; existing good texts
and original PDFs are never rewritten. Uses text layers, not OCR.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
from util.markdown_util import create_markdown_file, require_success, usable_text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--expected-count', type=int, required=True)
    parser.add_argument('--yes', action='store_true')
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    root = Path('/corpus-updater/corpora').resolve()
    target = (root / manifest['target']['name']).resolve()
    if target.parent != root or not target.is_dir():
        raise SystemExit('Invalid corpus target')
    jobs = []
    for item in manifest['items']:
        filename = item['target_file']
        if Path(filename).name != filename or not filename.endswith('.pdf'):
            raise SystemExit('Unsafe PDF filename in manifest')
        pdf = target / filename
        txt = target / 'markdown' / (pdf.stem + '.txt')
        if not pdf.is_file():
            raise SystemExit(f'Missing PDF: {filename}')
        if not usable_text(txt):
            jobs.append((str(target), str(target / 'markdown'), filename))
    if len(jobs) != args.expected_count:
        raise SystemExit(f'Expected {args.expected_count} repairs, found {len(jobs)}. Recheck before writing.')
    print(json.dumps({'repair_count': len(jobs), 'files': [j[2] for j in jobs], 'apply': args.yes}), flush=True)
    if not args.yes:
        return
    backup = target / ('.dt-markdown-backup-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ'))
    backup.mkdir()
    for _, md, filename in jobs:
        old = Path(md) / (Path(filename).stem + '.txt')
        if old.exists():
            shutil.copy2(old, backup / old.name)
    results = []
    for index, job in enumerate(jobs, 1):
        results.append(create_markdown_file(job, text_only=True))
        print(json.dumps({'completed': index, 'total': len(jobs)}), flush=True)
    (backup / 'report.json').write_text(json.dumps(results, ensure_ascii=True, indent=2), encoding='utf-8')
    require_success(results)
    print(json.dumps({'repaired': len(results), 'backup': str(backup)}), flush=True)


if __name__ == '__main__':
    main()
