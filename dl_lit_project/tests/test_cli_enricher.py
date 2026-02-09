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
def mock_searcher(mocker):
    """Mocks the OpenAlexCrossrefSearcher."""
    mock_searcher_class = mocker.patch('dl_lit.cli.OpenAlexCrossrefSearcher', autospec=True)
    instance = mock_searcher_class.return_value
    return instance

@pytest.fixture
def mock_process_single_reference(mocker):
    """Mocks the OpenAlexScraper.process_single_reference call used by the CLI."""
    return mocker.patch('dl_lit.cli.process_single_reference', autospec=True)


def test_enrich_empty_db(mock_db_manager, tmp_path):
    """Test that the command exits gracefully when no data needs processing."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        'enrich-openalex-db',
        '--db-path', str(tmp_path / 'test.db')
    ])

    assert result.exit_code == 0
    assert "No entries found in 'no_metadata' table to process." in result.output
    mock_db_manager.fetch_no_metadata_batch.assert_called_once()

def test_enrich_success(mock_db_manager, mock_searcher, mock_process_single_reference, tmp_path):
    """Test a successful enrichment workflow."""
    # 1. Setup Mocks
    mock_entry = {'id': 1, 'title': 'Test Title 1', 'doi': '10.1000/test', 'openalex_id': 'W123'}
    mock_db_manager.fetch_no_metadata_batch.return_value = [mock_entry]

    enrichment_result = {'title': 'Enriched Title'}
    mock_process_single_reference.return_value = enrichment_result
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

    mock_process_single_reference.assert_called_once()
    mock_db_manager.promote_to_with_metadata.assert_called_once()
    args, kwargs = mock_db_manager.promote_to_with_metadata.call_args
    assert args == (1, enrichment_result)
    assert kwargs.get("expand_related") is True
    assert kwargs.get("max_related_per_source") == 40

def test_enrich_partial_failure(mock_db_manager, mock_searcher, mock_process_single_reference, tmp_path):
    """Test workflow where some entries fail to enrich."""
    # 1. Setup Mocks
    mock_entries = [
        {'id': 1, 'title': 'Test Title 1', 'doi': '10.1000/test1', 'openalex_id': 'W123'},
        {'id': 2, 'title': 'Test Title 2', 'doi': '10.1000/test2', 'openalex_id': 'W456'}
    ]
    mock_db_manager.fetch_no_metadata_batch.return_value = mock_entries

    # Simulate one success, one failure by configuring side_effect
    enrichment_result = {'title': 'Enriched Title'}
    mock_process_single_reference.side_effect = [enrichment_result, None]
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
    mock_db_manager.promote_to_with_metadata.assert_called_once()
    args, kwargs = mock_db_manager.promote_to_with_metadata.call_args
    assert args == (1, enrichment_result)
    assert kwargs.get("expand_related") is True
    assert kwargs.get("max_related_per_source") == 40
    mock_db_manager.move_no_meta_entry_to_failed.assert_called_once_with(
        2, 'Metadata fetch failed (no match found in OpenAlex/Crossref)'
    )
