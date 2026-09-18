import json
from pathlib import Path
import sqlite3
import subprocess
import sys


SCRIPT = Path(__file__).resolve().parents[2] / 'backend/scripts/ingest_stats.py'


def test_ingest_stats_keeps_totals_and_separates_active_work(tmp_path):
    database = tmp_path / 'stats.db'
    with sqlite3.connect(database) as conn:
        conn.executescript('''
            CREATE TABLE works (id INTEGER PRIMARY KEY, metadata_status TEXT, download_status TEXT);
            CREATE TABLE corpus_works (corpus_id INTEGER, work_id INTEGER);
            INSERT INTO works VALUES
              (1, 'pending', 'queued'), (2, 'in_progress', 'in_progress'),
              (3, 'matched', 'downloaded'), (4, 'in_progress', 'in_progress');
            INSERT INTO corpus_works VALUES (10, 1), (10, 2), (10, 3), (20, 4);
        ''')

    def read_stats(*args):
        result = subprocess.run([sys.executable, str(SCRIPT), '--db-path', str(database), *args],
                                check=True, capture_output=True, text=True)
        return json.loads(result.stdout)['stats']

    scoped = read_stats('--corpus-id', '10')
    assert scoped == {'raw_pending': 2, 'matched': 0, 'queued_download': 2,
                      'enriching': 1, 'downloading': 1, 'downloaded': 1}
    global_stats = read_stats()
    assert global_stats['raw_pending'] == 3
    assert global_stats['queued_download'] == 3
    assert global_stats['enriching'] == 2
    assert global_stats['downloading'] == 2
    assert all(value == 0 for value in read_stats('--corpus-id', '99').values())
