import json
import re
import time
from typing import Iterable
import requests

from .utils import get_global_rate_limiter

TOKEN_RE = re.compile(r'"[^"]+"|\(|\)|\bAND\b|\bOR\b|\bNOT\b|[^()\s]+', re.IGNORECASE)

PRECEDENCE = {
    'NOT': 3,
    'AND': 2,
    'OR': 1,
}


class QuerySyntaxError(ValueError):
    pass


def _tokenize(query: str) -> list[str]:
    tokens = TOKEN_RE.findall(query)
    if not tokens:
        raise QuerySyntaxError("Query is empty")
    normalized = []
    for token in tokens:
        upper = token.upper()
        if upper in ("AND", "OR", "NOT"):
            normalized.append(upper)
        else:
            normalized.append(token)
    return normalized


def _insert_implicit_and(tokens: list[str]) -> list[str]:
    """Insert implicit AND between adjacent terms or term/parenthesis pairs."""
    result = []
    def is_term(tok: str) -> bool:
        return tok not in ("AND", "OR", "NOT", "(", ")")

    prev = None
    for tok in tokens:
        if prev is not None:
            if (is_term(prev) or prev == ")") and (is_term(tok) or tok == "(" or tok == "NOT"):
                result.append("AND")
        result.append(tok)
        prev = tok
    return result


def normalize_query(query: str) -> str:
    """Validate and normalize a boolean query string.

    Returns a normalized query with explicit ANDs and uppercase operators.
    """
    tokens = _insert_implicit_and(_tokenize(query))

    # Shunting-yard validation (no evaluation)
    output: list[str] = []
    stack: list[str] = []
    for tok in tokens:
        if tok in PRECEDENCE:
            while stack and stack[-1] in PRECEDENCE and PRECEDENCE[stack[-1]] >= PRECEDENCE[tok]:
                output.append(stack.pop())
            stack.append(tok)
        elif tok == "(":
            stack.append(tok)
        elif tok == ")":
            while stack and stack[-1] != "(":
                output.append(stack.pop())
            if not stack:
                raise QuerySyntaxError("Mismatched parentheses")
            stack.pop()
        else:
            output.append(tok)

    while stack:
        if stack[-1] in ("(", ")"):
            raise QuerySyntaxError("Mismatched parentheses")
        output.append(stack.pop())

    # Return normalized query string
    return " ".join(tokens)


def _openalex_request(params: dict, rate_limiter, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            rate_limiter.wait_if_needed('openalex')
            response = requests.get("https://api.openalex.org/works", params=params, timeout=30)
            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.RequestException(f"HTTP {response.status_code}")
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("OpenAlex request failed after retries")


def search_openalex(query: str,
                    max_results: int = 200,
                    year_from: int | None = None,
                    year_to: int | None = None,
                    mailto: str | None = None,
                    field: str | None = "default") -> list[dict]:
    normalized_query = normalize_query(query)
    rate_limiter = get_global_rate_limiter()

    params = {
        "per-page": 200,
        "select": "id,doi,display_name,authorships,publication_year,type,abstract_inverted_index,keywords,primary_location,open_access,biblio",
    }
    if mailto:
        params["mailto"] = mailto

    field_key = None
    if field:
        field_key = {
            "default": None,
            "search": None,
            "title": "title.search",
            "abstract": "abstract.search",
            "title_and_abstract": "title_and_abstract.search",
            "fulltext": "fulltext.search",
        }.get(field.strip().lower())
        if field_key is None and field.strip().lower() not in ("default", "search"):
            raise ValueError(f"Unknown search field: {field}")

    if field_key:
        params["filter"] = f"{field_key}:{normalized_query}"
    else:
        params["search"] = normalized_query

    filters = []
    if year_from:
        filters.append(f"publication_year:>={year_from}")
    if year_to:
        filters.append(f"publication_year:<={year_to}")
    if filters:
        if "filter" in params:
            params["filter"] = ",".join([params["filter"], *filters])
        else:
            params["filter"] = ",".join(filters)

    # Use cursor-based pagination for robustness
    params["cursor"] = "*"
    results: list[dict] = []
    seen_ids: set[str] = set()

    while True:
        data = _openalex_request(params, rate_limiter)
        for item in data.get("results", []):
            item_id = item.get("id")
            if not item_id or item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            results.append(item)
            if len(results) >= max_results:
                return results
        next_cursor = data.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor

    return results


def openalex_result_to_record(item: dict, run_id: int | None = None) -> dict:
    """Normalize OpenAlex result into a search result/queue record."""
    biblio = item.get("biblio") or {}
    primary_location = item.get("primary_location") or {}
    source_info = primary_location.get("source") or {}
    authors = [a.get("author", {}).get("display_name") for a in item.get("authorships", [])]

    first_page = biblio.get("first_page")
    last_page = biblio.get("last_page")
    pages = f"{first_page}--{last_page}" if first_page and last_page else None

    return {
        "openalex_id": item.get("id"),
        "doi": item.get("doi"),
        "title": item.get("display_name"),
        "year": item.get("publication_year"),
        "authors": authors,
        "abstract": item.get("abstract_inverted_index"),
        "keywords": [kw.get("display_name") for kw in item.get("keywords", []) if kw.get("display_name")],
        "source": source_info.get("display_name"),
        "volume": biblio.get("volume"),
        "issue": biblio.get("issue"),
        "pages": pages,
        "publisher": source_info.get("publisher"),
        "type": item.get("type"),
        "url": item.get("id"),
        "open_access_url": item.get("open_access", {}).get("oa_url"),
        "openalex_json": item,
        "ingest_source": "keyword_search",
        "run_id": run_id,
    }


def dedupe_results(results: Iterable[dict]) -> list[dict]:
    seen = set()
    deduped = []
    for result in results:
        key = result.get("openalex_id") or result.get("doi") or result.get("title")
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(result)
    return deduped
