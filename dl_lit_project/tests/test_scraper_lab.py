from pathlib import Path

from dl_lit.scraper_lab import build_artifact_record, build_eurlex_expert_query, safe_filename


def test_build_eurlex_query_escapes_text():
    query = build_eurlex_expert_query(
        {
            "fm_coded": "REG",
            "dd_year": "2024",
            "celex_id": "32024R1689",
            "lang": "EN",
            "text_query": "children's rights",
        }
    )

    assert "FM_CODED = REG" in query
    assert "DD_YEAR = 2024" in query
    assert "DN = 32024R1689" in query
    assert "AU = EN" in query
    assert "TI ~ 'children''s rights'" in query


def test_eurlex_artifact_record_uses_celex_key_and_pdf():
    record = build_artifact_record(
        "eurlex",
        {
            "celex": "32024R1689",
            "title": "Artificial Intelligence Act",
            "date": "2024-06-13",
            "pdf": "https://example.test/ai-act.pdf",
            "html": "https://example.test/ai-act.html",
        },
        1,
        Path("/tmp/scraper-run"),
    )

    assert record["source_id"] == "32024R1689"
    assert record["bibtex_key"] == "eurlex_32024R1689"
    assert record["year"] == 2024
    assert record["download_url"] == "https://example.test/ai-act.pdf"
    assert record["file_name"] == "eurlex_32024R1689.pdf"
    assert record["metadata_completeness"] == "minimal"


def test_expert_groups_artifact_record_keeps_minimal_metadata():
    record = build_artifact_record(
        "expert_groups",
        {
            "expert_group_id": "E03745",
            "group_title": "Example Group",
            "date": "17/05/2024",
            "title": "Meeting",
            "meeting_url": "https://ec.europa.eu/meeting",
            "doc_url": "https://ec.europa.eu/document/agenda.pdf",
            "doc_title": "Agenda",
        },
        1,
        Path("/tmp/scraper-run"),
    )

    assert record["bibtex_key"].startswith("expertgroups_E03745_17_05_2024_")
    assert record["title"] == "Agenda"
    assert record["year"] == 2024
    assert record["file_name"].endswith(".pdf")
    assert record["metadata_completeness"] == "minimal"


def test_safe_filename_has_fallback():
    assert safe_filename(" /bad:name* ") == "bad_name"
    assert safe_filename("???", fallback="fallback") == "fallback"
