from dl_lit.OpenAlexScraper import get_authors_and_editors


def test_get_authors_and_editors_parses_json_string_lists():
    ref = {
        "authors": '["Larry Downes", "Paul F. Nunes"]',
        "editors": '["Jane Editor"]',
    }

    authors, editors = get_authors_and_editors(ref)

    assert authors == ["Larry Downes", "Paul F. Nunes"]
    assert editors == ["Jane Editor"]


def test_get_authors_and_editors_parses_person_dicts():
    ref = {
        "authors": [
            {"given": "Catherine", "family": "Gwin"},
            {"display_name": "Frederick K. Lister"},
        ]
    }

    authors, editors = get_authors_and_editors(ref)

    assert authors == ["Gwin, Catherine", "Frederick K. Lister"]
    assert editors == []


def test_get_authors_and_editors_keeps_legacy_multi_author_split():
    ref = {
        "authors": ["Downes, Larry, Nunes, Paul F."],
    }

    authors, _ = get_authors_and_editors(ref)

    assert authors == ["Downes, Larry", "Nunes, Paul F."]
