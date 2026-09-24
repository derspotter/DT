"""DT's drop-in Kantropos markdown adapter. No OCR or embedding side effects.

Mount read-only over /corpus-updater/util/markdown_util.py. Preserve the
upstream public entry point, but never report empty/failed text as success.
"""
import json
import os
from pathlib import Path
import re
import tempfile
from collections import Counter
from multiprocessing import Pool, TimeoutError


def log(event, **fields):
    print(json.dumps({'stage': 'markdown', 'event': event, **fields}, ensure_ascii=True), flush=True)


def usable_text(path):
    try:
        return bool(Path(path).read_text(encoding='utf-8').strip())
    except (FileNotFoundError, UnicodeError):
        return False


def normalize_text(text):
    # Isolated PDF surrogate code points cannot be encoded in UTF-8. Mark
    # them explicitly instead of dropping characters or losing the document.
    return re.subn(r'[\ud800-\udfff]', '\ufffd', text)


def atomic_text(path, text):
    if not text.strip():
        raise ValueError('Refusing to save empty text')
    path = Path(path)
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix=f'.{path.name}.', suffix='.tmp', delete=False) as out:
            tmp = Path(out.name)
            out.write(text)
            out.flush()
            os.fsync(out.fileno())
        os.chmod(tmp, path.stat().st_mode & 0o777 if path.exists() else 0o644)
        os.replace(tmp, path)
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)


def extract_markdown(pdf):
    import pymupdf4llm
    return pymupdf4llm.to_markdown(str(pdf))


def extract_plain_text(pdf):
    import pymupdf
    # Reopen: layout conversion may have altered its in-memory document.
    # Page errors are not suppressed; partial documents must not pass.
    with pymupdf.open(pdf) as document:
        return '\n\n'.join(page.get_text(sort=True) for page in document)


def create_markdown_file(file_info, *, text_only=False):
    input_dir, markdown_dir, filename = file_info
    pdf = Path(input_dir) / filename
    output = Path(markdown_dir) / (Path(filename).stem + '.txt')
    result = {'file': filename}
    try:
        if usable_text(output):
            return {**result, 'status': 'existing'}
        reason = 'Explicit text-layer recovery of empty output' if text_only else None
        if not text_only:
            try:
                text = extract_markdown(pdf)
                if not text.strip():
                    reason = 'Markdown converter returned empty text'
            except Exception as exc:
                reason = f'{type(exc).__name__}: {exc}'
        if reason:
            text = extract_plain_text(pdf)
            if not text.strip():
                raise ValueError(f'{reason}; PDF text fallback is also empty. OCR/manual review required')
        text, replacements = normalize_text(text)
        atomic_text(output, text)
        result.update(status='fallback' if reason else 'created', chars=len(text),
                      replacement_characters=replacements)
        if reason:
            result['reason'] = reason
        log('file_complete', **result)
    except Exception as exc:
        result.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        log('file_failed', **result)
    return result


def create_markdown_directory(input_dir):
    path = Path(input_dir) / 'markdown'
    path.mkdir(exist_ok=True)
    return str(path)


def copy_txt_files_to_markdown_directory(input_dir, markdown_dir):
    for source in sorted(Path(input_dir).glob('*.txt')):
        text = source.read_text(encoding='utf-8')
        if not text.strip():
            raise ValueError(f'Empty source text sidecar: {source.name}')
        dest = Path(markdown_dir) / source.name
        try:
            if dest.read_text(encoding='utf-8') == text:
                continue
        except (FileNotFoundError, UnicodeError):
            # A valid OCR sidecar can replace missing/corrupt output too.
            pass
        atomic_text(dest, text)


def require_success(results):
    failed = [item for item in results if item['status'] == 'failed']
    if failed:
        raise RuntimeError(f'{len(failed)} markdown file(s) failed; embedding must not start. '
                           + json.dumps(failed, ensure_ascii=True))


def create_markdown_files(corpus):
    root = Path('corpora').resolve()
    input_dir = (root / corpus).resolve()
    if input_dir.parent != root or not input_dir.is_dir():
        raise ValueError('Invalid corpus directory')
    markdown_dir = create_markdown_directory(input_dir)
    copy_txt_files_to_markdown_directory(input_dir, markdown_dir)
    jobs = [(str(input_dir), markdown_dir, pdf.name) for pdf in sorted(input_dir.glob('*.pdf'))]
    workers = max(1, min(int(os.environ.get('DT_MARKDOWN_WORKERS', '4')), os.cpu_count() or 1))
    results, counts = [], Counter()
    log('start', corpus=corpus, total=len(jobs), workers=workers)
    with Pool(processes=workers) as pool:
        pending = pool.imap_unordered(create_markdown_file, jobs)
        while len(results) < len(jobs):
            try:
                result = pending.next(timeout=10)
            except TimeoutError:
                log('waiting', completed=len(results), total=len(jobs), counts=dict(counts))
                continue
            results.append(result)
            counts[result['status']] += 1
            if len(results) % 25 == 0 or len(results) == len(jobs):
                log('progress', completed=len(results), total=len(jobs), counts=dict(counts))
    require_success(results)
    log('complete', corpus=corpus, total=len(jobs), counts=dict(counts))
