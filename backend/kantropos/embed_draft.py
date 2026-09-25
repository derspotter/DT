"""Scoped INSERT runner, executed inside the existing Kantropos updater.

Read a DT manifest from stdin. Preview by default, write only with --yes.
Never call the upstream corpus-wide /embeddings endpoint from this runner.
"""
import argparse
import json
import math
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


def scope_from_manifest(manifest):
    corpus = manifest.get('target', {}).get('name')
    if not isinstance(corpus, str) or not corpus.strip() or corpus in {'.', '..'} or any(c in corpus for c in '/\\\x00'):
        raise ValueError('Manifest must name one corpus directory')
    items = manifest.get('items')
    if not isinstance(items, list) or not items:
        raise ValueError('Manifest must contain a nonempty items list')
    names = []
    for item in items:
        name = item.get('target_file') if isinstance(item, dict) else None
        if not isinstance(name, str) or not name.endswith('.pdf') or any(c in name for c in '/\\\x00'):
            raise ValueError(f'Unsafe or missing target_file: {name!r}')
        names.append(name)
    if len(set(names)) != len(names):
        raise ValueError('Duplicate target_file in manifest')
    return corpus, names


def read_text(root, name):
    # Reject symlinks escaping this corpus, including the markdown directory.
    for path in (root / name, root / 'markdown' / (name[:-4] + '.txt')):
        if not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
            raise ValueError(f'{name}: missing file or path outside corpus: {path.name}')
    try:
        text = (root / 'markdown' / (name[:-4] + '.txt')).read_text(encoding='utf-8')
    except UnicodeError as exc:
        raise ValueError(f'{name}: text is not valid UTF-8') from exc
    if not text.strip():
        raise ValueError(f'{name}: empty Markdown text, embedding has not been requested')
    return text


def checked_embedding(text, request):
    if not text.strip():
        raise ValueError('Empty embedding input, refusing Ollama request')
    result = request(text)
    vectors = result.embeddings
    if len(vectors) != 1 or not vectors[0] or not all(math.isfinite(v) for v in vectors[0]):
        raise ValueError('Ollama returned no valid embedding for nonempty text')
    return vectors[0]


class Runtime:
    """Reuse the installed upstream model, document format and metadata writer."""

    def __init__(self, corpus):
        from config import embed_configs
        import psycopg2
        from util.metadata_util import MetadataUpdater

        self.config = embed_configs
        if embed_configs.model_name == 'OpenAIEmbedding' or embed_configs.model_name.startswith('jinaai/'):
            raise ValueError('Scoped runner currently supports the configured Ollama model only')
        self.corpus = corpus
        self.root = Path('/corpus-updater/corpora') / corpus
        if self.root.resolve().parent != Path('/corpus-updater/corpora').resolve():
            raise ValueError('Corpus directory resolves outside the corpora root')
        self.collection = f'Collection_{corpus}_embedding_' + re.sub(r'[/:]', '-', embed_configs.model_name)
        self.metadata = MetadataUpdater(corpus)
        self.db = psycopg2.connect(dbname='kantropos', user='kantropos-user',
                                  password=os.environ['POSTGRES_PASSWORD'], host='db', port=5432)
        self.db.set_session(readonly=True, autocommit=True)
        self.index = None
        self.writer_connected = False

    def existing_ids(self, names):
        # Exact aggregation avoids the upstream 10-vector-at-a-time full scan.
        # doc_id is the payload field written by upstream's QdrantVectorStore.
        # Huge MatchAny facets can consume excessive memory in older Qdrant.
        # Avoid that query shape, even for a read-only preview.
        # Read only distinct index IDs, then intersect locally, never document text.
        limit = 100000
        payload = {'key': 'doc_id', 'limit': limit, 'exact': True}
        url = 'http://qdrant:6333/collections/' + urllib.parse.quote(self.collection, safe='') + '/facet'
        request = urllib.request.Request(url, data=json.dumps(payload).encode(), headers={
            'Content-Type': 'application/json', 'api-key': os.environ['QDRANT__SERVICE__API_KEY']})
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                result = json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return set()  # First import: QdrantVectorStore creates the collection on insert.
            raise
        # Never fall back to a corpus-wide import on an API error.
        hits = result['result']['hits']
        if len(hits) >= limit:
            raise ValueError('Index ID inventory reached safety limit, refusing an incomplete resume check')
        return {hit['value'] for hit in hits} & set(names)

    def metadata_ids(self):
        with self.db.cursor() as cursor:
            cursor.execute('SELECT key FROM metadata WHERE corpus=%s', (self.corpus,))
            return {row[0] for row in cursor.fetchall()}

    def prepare(self, name):
        from llama_index.core import Document
        from llama_index.core.schema import MetadataMode
        from util.llama_index_util import CustomSentenceSplitter

        text = read_text(self.root, name)
        title, year, authors = self.metadata.extract_title_author_year_from_metadata_for_given_pdf(name)
        metadata = {'filename': name, 'title': title, 'year': year,
                    'author': '; '.join(str(a) for a in authors)}
        doc = Document(text=text, metadata=metadata, doc_id=name,
                       excluded_embed_metadata_keys=list(metadata), excluded_llm_metadata_keys=['filename'],
                       text_template='<metadata>\n{metadata_str}\n<content>\n{content}')
        splitter = CustomSentenceSplitter(chunk_size=self.config.chunk_size, chunk_overlap=self.config.chunk_overlap)
        nodes = splitter.get_nodes_from_documents([doc], show_progress=False)
        if not nodes or any(not node.get_content(metadata_mode=MetadataMode.EMBED).strip() for node in nodes):
            raise ValueError('Document produces an empty embedding chunk')
        return nodes

    def insert(self, name, nodes):
        if self.index is None:
            from llama_index.core import VectorStoreIndex
            from llama_index.embeddings.ollama import OllamaEmbedding
            from util.database_util import create_client, get_vector_store

            class CheckedOllamaEmbedding(OllamaEmbedding):
                def get_general_text_embedding(self, text):
                    return checked_embedding(text, lambda value: self._client.embed(
                        model=self.model_name, input=value, options=self.ollama_additional_kwargs))

            model = CheckedOllamaEmbedding(model_name=self.config.model_name, base_url=self.config.base_url,
                                           embed_batch_size=self.config.embed_batch_size)
            create_client()
            self.index = VectorStoreIndex.from_vector_store(
                vector_store=get_vector_store(self.collection), embed_model=model)
            self.metadata.connect_to_db()
            self.writer_connected = True
        self.index.insert_nodes(nodes)
        self.metadata.update_metadata_entry(name)
        self.metadata.commit_db_changes()

    def close(self):
        if self.writer_connected:
            self.metadata.disconnect_from_db()
        self.db.close()


def run(manifest, *, yes=False, runtime_factory=Runtime):
    corpus, names = scope_from_manifest(manifest)
    runtime = runtime_factory(corpus)
    try:
        wanted = set(names)
        existing = runtime.existing_ids(names) & wanted
        metadata = runtime.metadata_ids() & wanted
        if existing != metadata:
            raise ValueError('Vector/metadata mismatch within draft. Repair before resuming. '
                             f'Vector-only: {sorted(existing - metadata)[:5]}. '
                             f'Metadata-only: {sorted(metadata - existing)[:5]}')
        pending = [name for name in names if name not in existing]
        print(json.dumps({'corpus': corpus, 'draft_documents': len(names), 'already_indexed': len(existing),
                          'pending': len(pending), 'write': yes}), flush=True)
        # Check all pending text before any vector writes, never enumerate old PDFs.
        for name in pending:
            read_text(runtime.root, name)
        for position, name in enumerate(pending, 1):
            print(f'[{position}/{len(pending)}] {"Embedding" if yes else "Checking"} {name}', flush=True)
            try:
                nodes = runtime.prepare(name)
                if yes:
                    runtime.insert(name, nodes)
            except Exception as exc:
                raise RuntimeError(f'{name}: {type(exc).__name__}: {exc}. Stopped, no automatic retry. '
                                   'Check vector/metadata consistency before resuming.') from exc
        if yes:
            vectors = runtime.existing_ids(names) & wanted
            metadata = runtime.metadata_ids() & wanted
            if vectors != wanted or metadata != wanted:
                raise RuntimeError('Completion audit failed: not every draft document has vectors and metadata')
            print(f'COMPLETE: {len(names)} draft documents have vectors and metadata.', flush=True)
        else:
            print(f'PREVIEW ONLY: {len(pending)} pending documents checked. No embeddings or database writes.', flush=True)
    finally:
        runtime.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--yes', action='store_true', help='Write missing draft documents, otherwise preview only')
    args = parser.parse_args()
    run(json.load(sys.stdin), yes=args.yes)
