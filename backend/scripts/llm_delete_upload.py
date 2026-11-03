"""Delete a previously uploaded LLM file: llm_delete_upload.py <name>. Prints "ok"."""

import sys

from _bootstrap import ensure_import_paths

ensure_import_paths(__file__)


def main(argv, client=None):
    if client is None:
        from dl_lit.llm_provider import get_client
        client = get_client(required=True)
    client.files.delete(name=argv[0])
    print("ok")


if __name__ == '__main__':
    main(sys.argv[1:])
