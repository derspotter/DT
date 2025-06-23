import pytest
from click.testing import CliRunner

from dl_lit.cli import cli

@pytest.fixture
def mock_db_manager(mocker):
    """Mocks the DatabaseManager, returning an empty batch by default."""
    mock_db_class = mocker.patch('dl_lit.cli.DatabaseManager', autospec=True)
    instance = mock_db_class.return_value
    instance.fetch_no_metadata_batch.return_value = []
    # Configure the mock for the new method to ensure the attribute exists.
    instance.move_no_meta_entry_to_failed = mocker.Mock(return_value=(1, None))
    return instance

@pytest.fixture
def mock_metadata_fetcher(mocker):
    """Mocks the MetadataFetcher."""
    mock_fetcher_class = mocker.patch('dl_lit.cli.MetadataFetcher', autospec=True)
    instance = mock_fetcher_class.return_value
    return instance


def test_enrich_empty_db(mock_db_manager, tmp_path):
    """Test that the command exits gracefully when no data needs processing."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        'enrich-openalex-db',
        '--db-path', str(tmp_path / 'test.db')
    ])

    assert result.exit_code == 0
    assert "No rows in no_metadata – nothing to enrich." in result.output
    mock_db_manager.fetch_no_metadata_batch.assert_called_once()

def test_enrich_success(mock_db_manager, mock_metadata_fetcher, tmp_path):
    """Test a successful enrichment workflow."""
    # 1. Setup Mocks
    mock_entry = {'id': 1, 'title': 'Test Title 1', 'doi': '10.1000/test', 'openalex_id': 'W123'}
    mock_db_manager.fetch_no_metadata_batch.return_value = [mock_entry]

    enrichment_result = {'title': 'Enriched Title'}
    mock_metadata_fetcher.fetch_metadata.return_value = enrichment_result
    mock_db_manager.promote_to_with_metadata.return_value = (1, None)

    # 2. Run Command
    runner = CliRunner()
    result = runner.invoke(cli, [
        'enrich-openalex-db',
        '--db-path', str(tmp_path / 'test.db')
    ])

    # 3. Assertions
    assert result.exit_code == 0
    assert "Enrichment finished: 1 promoted, 0 failed." in result.output

    mock_metadata_fetcher.fetch_metadata.assert_called_once_with(mock_entry)
    mock_db_manager.promote_to_with_metadata.assert_called_once_with(1, enrichment_result)

def test_enrich_partial_failure(mock_db_manager, mock_metadata_fetcher, tmp_path):
    """Test workflow where some entries fail to enrich."""
    # 1. Setup Mocks
    mock_entries = [
        {'id': 1, 'title': 'Test Title 1', 'doi': '10.1000/test1', 'openalex_id': 'W123'},
        {'id': 2, 'title': 'Test Title 2', 'doi': '10.1000/test2', 'openalex_id': 'W456'}
    ]
    mock_db_manager.fetch_no_metadata_batch.return_value = mock_entries

    # Simulate one success, one failure by configuring side_effect
    enrichment_result = {'title': 'Enriched Title'}
    mock_metadata_fetcher.fetch_metadata.side_effect = [enrichment_result, None]
    mock_db_manager.promote_to_with_metadata.return_value = (1, None)

    # 2. Run Command
    runner = CliRunner()
    result = runner.invoke(cli, [
        'enrich-openalex-db',
        '--db-path', str(tmp_path / 'test.db')
    ])

    # 3. Assertions
    assert result.exit_code == 0
    assert "Enrichment finished: 1 promoted, 1 failed." in result.output

    # Assertions
    mock_db_manager.promote_to_with_metadata.assert_called_once_with(1, enrichment_result)
    mock_db_manager.move_no_meta_entry_to_failed.assert_called_once_with(2, 'metadata_fetch_failed')

