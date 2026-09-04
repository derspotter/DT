from dl_lit import OpenAlexScraper


def test_referenced_work_details_carry_counts(monkeypatch):
    captured = {}

    def fake_request(**kwargs):
        captured['url'] = kwargs['url']
        return {"results": [{
            "id": "https://openalex.org/W1", "display_name": "A", "authorships": [],
            "publication_year": 2001, "doi": None, "type": "article",
            "referenced_works_count": 12, "cited_by_count": 345,
        }]}

    monkeypatch.setattr(OpenAlexScraper, "openalex_request_json", fake_request)
    details = OpenAlexScraper.fetch_referenced_work_details(["W1"], OpenAlexScraper.get_global_rate_limiter())
    assert "referenced_works_count" in captured['url'] and "cited_by_count" in captured['url']
    assert details[0]["referenced_works_count"] == 12
    assert details[0]["cited_by_count"] == 345
