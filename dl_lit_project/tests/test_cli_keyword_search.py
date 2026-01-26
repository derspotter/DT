import pytest
from click.testing import CliRunner

from dl_lit.cli import cli


@pytest.fixture
def mock_db_manager(mocker):
    mock_db_class = mocker.patch('dl_lit.cli.DatabaseManager', autospec=True)
    instance = mock_db_class.return_value
    instance.create_search_run.return_value = 1
    instance.add_search_results.return_value = 2
    return instance


@pytest.fixture
def mock_search(mocker):
    return mocker.patch('dl_lit.keyword_search.search_openalex', autospec=True)


@pytest.fixture
def mock_transform(mocker):
    return mocker.patch('dl_lit.keyword_search.openalex_result_to_record', autospec=True)


@pytest.fixture
def mock_dedupe(mocker):
    return mocker.patch('dl_lit.keyword_search.dedupe_results', autospec=True)


def test_keyword_search_basic(mock_db_manager, mock_search, mock_transform, mock_dedupe):
    runner = CliRunner()

    mock_search.return_value = [
        {"id": "W1", "display_name": "Title 1"},
        {"id": "W2", "display_name": "Title 2"},
    ]

    mock_transform.side_effect = [
        {"openalex_id": "W1", "openalex_json": {"id": "W1"}},
        {"openalex_id": "W2", "openalex_json": {"id": "W2"}},
    ]
    mock_dedupe.return_value = [
        {"openalex_id": "W1", "openalex_json": {"id": "W1"}},
        {"openalex_id": "W2", "openalex_json": {"id": "W2"}},
    ]

    result = runner.invoke(cli, [
        'keyword-search',
        '--query', 'foo AND bar',
        '--max-results', '2'
    ])

    assert result.exit_code == 0
    assert "Search run created" in result.output
    assert "Stored 2 search results." in result.output

    mock_db_manager.create_search_run.assert_called_once()
    mock_search.assert_called_once()
    mock_transform.assert_called()
    mock_dedupe.assert_called_once()


def test_keyword_search_enqueue(mock_db_manager, mock_search, mock_transform, mock_dedupe):
    runner = CliRunner()

    mock_search.return_value = [
        {"id": "W1", "display_name": "Title 1"},
    ]
    mock_transform.return_value = {"openalex_id": "W1", "openalex_json": {"id": "W1"}}
    mock_dedupe.return_value = [
        {"openalex_id": "W1", "openalex_json": {"id": "W1"}},
    ]

    mock_db_manager.add_entry_to_download_queue.return_value = ("added_to_queue", 1, "ok")

    result = runner.invoke(cli, [
        'keyword-search',
        '--query', 'foo',
        '--enqueue'
    ])

    assert result.exit_code == 0
    assert "Enqueue complete: 1 added, 0 skipped." in result.output
    mock_db_manager.add_entry_to_download_queue.assert_called_once()
