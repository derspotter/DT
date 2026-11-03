"""Upload a PDF to the configured LLM provider: llm_upload_pdf.py <pdf>.

Prints {"name": <uploaded file name>}. Non-ASCII paths are copied to an ASCII
temp name first, because the Gemini upload rejects non-ASCII filenames.
"""

import json
import os
import shutil
import sys
import tempfile
import unicodedata

from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)


def main(argv, client=None):
    from dl_lit.llm_provider import OpenAICompatClient, get_client

    if client is None:
        client = get_client(required=True)
    pdf_path = argv[0]
    base = os.path.basename(pdf_path)
    if isinstance(client, OpenAICompatClient):
        # Local handles must reference a persistent path; no remote upload happens.
        f = client.files.upload(file=pdf_path, config={"mime_type": "application/pdf", "display_name": base})
        print(json.dumps({"name": f.name}))
        return
    norm = unicodedata.normalize("NFKD", base).encode("ascii", "ignore").decode("ascii")
    safe = "".join((c if (c.isalnum() or c in "._-") else "_") for c in norm).strip("._") or "upload"
    if not safe.lower().endswith(".pdf"):
        safe += ".pdf"
    is_ascii = all(ord(c) < 128 for c in pdf_path) and all(ord(c) < 128 for c in base)
    tmpdir = None
    upload_path = pdf_path
    display_name = base
    if not is_ascii:
        tmpdir = tempfile.mkdtemp(prefix="gemini_upload_")
        upload_path = os.path.join(tmpdir, safe)
        shutil.copy2(pdf_path, upload_path)
        display_name = safe
    try:
        f = client.files.upload(file=upload_path, config={"mime_type": "application/pdf", "display_name": display_name})
        print(json.dumps({"name": f.name}))
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    main(sys.argv[1:])
