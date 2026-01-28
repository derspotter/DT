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
    instance.get_entries_to_download.return_value = []  # Default to empty
    instance.check_if_exists.return_value = (None, None, None)
    return instance

@pytest.fixture
def mock_enhancer(mocker):
    """Mocks the BibliographyEnhancer class and its instance methods."""
    mock_enhancer_class = mocker.patch('dl_lit.cli.BibliographyEnhancer', autospec=True)
    instance = mock_enhancer_class.return_value
    return instance

@pytest.fixture
def mock_hashlib(mocker):
    """Mocks the hashlib library to control checksum calculation."""
    mock_hl = mocker.patch('dl_lit.cli.hashlib', autospec=True)
    mock_hl.sha256.return_value.hexdigest.return_value = 'fake_checksum'
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
    mock_db_manager.get_entries_to_download.assert_called_once()
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
    mock_db_manager.get_entries_to_download.return_value = [mock_entry]

    enhancer_result = {
        'downloaded_file': str(pdf_file),
        'download_source': 'mock_source',
    }
    mock_enhancer.enhance_bibliography.return_value = enhancer_result

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
    assert "Worker finished: 1 downloaded, 0 failed." in result.output

    mock_enhancer.enhance_bibliography.assert_called_once()
    mock_hashlib.sha256.assert_called_once_with(b'pdf content')
    mock_db_manager.move_entry_to_downloaded.assert_called_once_with(
        1, enhancer_result, str(pdf_file), 'fake_checksum', 'mock_source'
    )

def test_download_pdfs_failure(mock_db_manager, mock_enhancer, tmp_path):
    """Test the workflow for a failed download."""
    # 1. Setup Mocks
    mock_entry = {'id': 2, 'title': 'Another Paper'}
    mock_db_manager.get_entries_to_download.return_value = [mock_entry]
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
    assert "Worker finished: 0 downloaded, 1 failed." in result.output

    mock_db_manager.move_queue_entry_to_failed.assert_called_once_with(2, 'download_failed')
