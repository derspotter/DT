import pytest
from click.testing import CliRunner
from pathlib import Path

from dl_lit.cli import cli

# Fixtures to mock dependencies used by the download_pdfs_command

@pytest.fixture
def mock_db_manager(mocker):
    """Mocks the DatabaseManager class and its instance methods."""
    mock_db_class = mocker.patch('dl_lit.cli.DatabaseManager', autospec=True)
    instance = mock_db_class.return_value
    instance.claim_download_batch.return_value = []  # Default to empty
    instance.find_downloaded_duplicate.return_value = (None, None, None)
    instance.move_entry_to_downloaded.return_value = (1, None)
    instance.mark_download_failed.return_value = (1, None)
    return instance


def _queue(db, *rows):
    """claim_download_batch hands out one row per call, then nothing."""
    db.claim_download_batch.side_effect = [[row] for row in rows] + [[]]


def _cli_id(db):
    return db.claim_download_batch.call_args.kwargs["claimed_by"]

@pytest.fixture
def mock_enhancer(mocker):
    """Mocks the BibliographyEnhancer class and its instance methods."""
    mock_enhancer_class = mocker.patch('dl_lit.cli.BibliographyEnhancer', autospec=True)
    instance = mock_enhancer_class.return_value
    return instance

@pytest.fixture
def mock_hashlib(mocker):
    """Mocks hashlib in the shared download path to control the fallback checksum."""
    mock_hl = mocker.patch('dl_lit.download_path.hashlib', autospec=True)
    mock_hl.file_digest.return_value.hexdigest.return_value = 'fake_checksum'
    return mock_hl

# Test cases

def test_download_pdfs_empty_queue(mock_db_manager, tmp_path):
    """Verify the command handles an empty download queue gracefully."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        'download-pdfs',
        '--db-path', str(tmp_path / 'test.db'),
        '--download-dir', str(tmp_path)
    ])

    assert result.exit_code == 0
    assert "Queue is empty." in result.output
    mock_db_manager.claim_download_batch.assert_called_once()
    assert mock_db_manager.claim_download_batch.call_args.kwargs["limit"] == 1
    mock_db_manager.release_download_claims.assert_called_once_with(_cli_id(mock_db_manager))
    mock_db_manager.close_connection.assert_called_once()

def test_download_pdfs_success(mocker, mock_db_manager, mock_enhancer, mock_hashlib, tmp_path):
    """Test the full workflow for a successful download."""
    download_dir = tmp_path / "downloads"
    pdf_file = download_dir / "paper.pdf"

    # 1. Setup Mocks
    mock_entry = {
        'id': 1,
        'title': 'A Test Paper',
        'authors': '["Author, A."]',
        'doi': '10.1000/test',
        'entry_type': 'article'
    }
    _queue(mock_db_manager, mock_entry)

    staged = {}

    def stage(ref, download_dir):
        # Like the real builders: the staged name <final><part_suffix>.
        staged['path'] = str(pdf_file) + ref['part_suffix']
        staged['result'] = {'downloaded_file': staged['path'], 'download_source': 'mock_source'}
        return staged['result']

    mock_enhancer.enhance_bibliography.side_effect = stage

    # Mock `open` to simulate reading the downloaded file for checksum
    mocker.patch('builtins.open', mocker.mock_open(read_data=b'pdf content'))

    # 2. Run Command
    runner = CliRunner()
    result = runner.invoke(cli, [
        'download-pdfs',
        '--db-path', str(tmp_path / 'test.db'),
        '--download-dir', str(download_dir)
    ])

    # 3. Assertions
    assert result.exit_code == 0
    assert "✓ downloaded via mock_source" in result.output
    assert "Worker finished: 1 downloaded, 0 failed, 0 duplicate(s) merged, 0 claim(s) lost (result discarded)." in result.output

    mock_enhancer.enhance_bibliography.assert_called_once()
    mock_hashlib.file_digest.assert_called_once()
    assert mock_hashlib.file_digest.call_args.args[1] == 'sha256'
    assert staged['path'].startswith(str(pdf_file) + '.part-')
    mock_db_manager.move_entry_to_downloaded.assert_called_once_with(
        1, staged['result'], staged['path'], 'fake_checksum', 'mock_source',
        claimed_by=_cli_id(mock_db_manager), part_path=staged['path'],
    )


def test_download_pdfs_unstaged_result_is_a_failed_download(mock_db_manager, mock_enhancer, tmp_path):
    """A downloader that ignores the requested suffix is not recorded: the row fails, under the claim."""
    pdf = tmp_path / "paper.pdf"
    pdf.write_bytes(b"%PDF published elsewhere")
    _queue(mock_db_manager, {'id': 3, 'title': 'Unstaged'})
    mock_enhancer.enhance_bibliography.return_value = {
        'downloaded_file': str(pdf), 'download_source': 'mock', 'checksum': 'abc',
    }

    result = CliRunner().invoke(cli, [
        'download-pdfs', '--db-path', str(tmp_path / 'test.db'), '--download-dir', str(tmp_path)
    ])

    assert result.exit_code == 0, result.output
    assert "unstaged file" in result.output
    assert "Worker finished: 0 downloaded, 1 failed, 0 duplicate(s) merged, 0 claim(s) lost (result discarded)." in result.output
    mock_db_manager.move_entry_to_downloaded.assert_not_called()
    mock_db_manager.mark_download_failed.assert_called_once_with(3, 'unstaged_download', claimed_by=_cli_id(mock_db_manager))
    assert pdf.read_bytes() == b"%PDF published elsewhere"  # never removed: not a staged file

def test_download_pdfs_failure(mock_db_manager, mock_enhancer, tmp_path):
    """Test the workflow for a failed download."""
    # 1. Setup Mocks
    mock_entry = {'id': 2, 'title': 'Another Paper'}
    _queue(mock_db_manager, mock_entry)
    mock_enhancer.enhance_bibliography.return_value = None  # Simulate failure

    # 2. Run Command
    runner = CliRunner()
    result = runner.invoke(cli, [
        'download-pdfs',
        '--db-path', str(tmp_path / 'test.db'),
        '--download-dir', str(tmp_path)
    ])

    # 3. Assertions
    assert result.exit_code == 0
    assert "✗ download failed" in result.output
    assert "Worker finished: 0 downloaded, 1 failed, 0 duplicate(s) merged, 0 claim(s) lost (result discarded)." in result.output

    mock_db_manager.mark_download_failed.assert_called_once_with(2, 'download_failed', claimed_by=_cli_id(mock_db_manager))
