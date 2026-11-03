"""Shared identifier helpers: DOIs, OpenAlex work ids, abstracts, OpenAlex records.

Stdlib only, so the standalone scripts in ``backend/scripts`` can import it.

The former copies did not agree with one another, so each distinct rule keeps
its own name instead of being merged into one "best" normaliser.  The outputs
feed DB dedupe keys (``normalized_doi``, ``openalex_id``), so every rule here
reproduces its former copies byte for byte;
``tests/fixtures/identifier_cases.json`` pins them.  The only deliberate
widening: a non-string input that the old copy rejected with an exception is
now coerced with ``str()`` first.
"""

from __future__ import annotations

import re

_DOI_IN_TEXT_RE = re.compile(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", re.IGNORECASE)
_DOI_TOKEN_RE = re.compile(r"10\.\d{4,9}/\S+", re.IGNORECASE)
_DOI_URL_PREFIX_RE = re.compile(r"^(https?://)?(dx\.)?doi\.org/")
_DOI_SCHEME_PREFIX_RE = re.compile(r"^doi:\s*")
_OPENALEX_WORK_IN_TEXT_RE = re.compile(r"(W\d+)", re.IGNORECASE)
_OPENALEX_WORK_TOKEN_RE = re.compile(r"^W\d+$", re.IGNORECASE)

__all__ = [
    "normalize_doi",
    "normalize_doi_lower",
    "extract_doi_lower",
    "normalize_openalex_id",
    "normalize_openalex_id_strict",
    "inverted_index_to_text",
    "inverted_index_to_text_last_wins",
    "inverted_index_to_text_lenient",
    "openalex_work_to_record",
    "crossref_item_to_openalex_like",
]


def normalize_doi(value, *, case: str = "upper") -> str | None:
    """The ``works.normalized_doi`` rule (formerly ``DatabaseManager._normalize_doi``).

    Extracts the first ``10.NNNN/...`` run anywhere in the value (URL or
    ``doi:`` prefixes fall away); without one, the whole input is kept as is,
    untrimmed.  ``case`` is ``"upper"`` (the DB key) or ``"lower"``.
    """
    if case not in ("upper", "lower"):
        raise ValueError(f"case must be 'upper' or 'lower', not {case!r}")
    if not value:
        return None
    text = str(value)
    match = _DOI_IN_TEXT_RE.search(text)
    result = match.group(1) if match else text
    return result.upper() if case == "upper" else result.lower()


def normalize_doi_lower(value) -> str | None:
    """Lowercase, drop a ``doi.org/`` URL or ``doi:`` prefix, trim `` .;,``; no validation.

    Formerly ``OpenAlexScraper.normalize_doi``.
    """
    if not value:
        return None
    doi = str(value).strip().lower()
    doi = _DOI_URL_PREFIX_RE.sub("", doi)
    doi = _DOI_SCHEME_PREFIX_RE.sub("", doi)
    doi = doi.strip().strip(" .;,")
    return doi or None


def extract_doi_lower(value) -> str | None:
    """Lowercase, drop ``doi.org`` / ``doi:`` prefixes, return the first ``10.NNNN/\\S+`` or ``None``.

    Formerly ``backend/scripts/keyword_search.normalize_doi``.
    """
    if not value:
        return None
    raw = str(value).strip().lower()
    raw = raw.replace("https://doi.org/", "").replace("http://doi.org/", "")
    raw = raw.replace("doi:", "")
    match = _DOI_TOKEN_RE.search(raw)
    return match.group(0) if match else None


def normalize_openalex_id(value) -> str | None:
    """The ``works.openalex_id`` rule: the first ``W<digits>`` anywhere, uppercased, else ``None``.

    Formerly ``DatabaseManager._normalize_openalex_id`` and the script copies.
    """
    if not value:
        return None
    match = _OPENALEX_WORK_IN_TEXT_RE.search(str(value))
    return match.group(1).upper() if match else None


def normalize_openalex_id_strict(value) -> str | None:
    """The value (or the last segment of an ``http`` URL) must be exactly ``W<digits>``; uppercased.

    Formerly ``backend/scripts/keyword_search.normalize_openalex_id``.
    """
    if not value:
        return None
    raw = str(value).strip()
    if raw.startswith("http"):
        raw = raw.rstrip("/").split("/")[-1]
    return raw.upper() if _OPENALEX_WORK_TOKEN_RE.match(raw) else None


def inverted_index_to_text(idx) -> str | None:
    """OpenAlex ``abstract_inverted_index`` -> text, words ordered by position.

    Empty or ``None`` -> ``None``; words sharing a position are all kept, in
    dict order.  Formerly the effective
    ``DatabaseManager._convert_inverted_index_to_text``.
    """
    if not idx:
        return None
    word_positions = []
    for word, positions in idx.items():
        for position in positions:
            word_positions.append((word, position))
    word_positions.sort(key=lambda pair: pair[1])
    return " ".join(word for word, _ in word_positions)


def inverted_index_to_text_last_wins(idx) -> str | None:
    """Like :func:`inverted_index_to_text`, but one word per position: the last one wins.

    Formerly ``OpenAlexScraper.get_abstract``.
    """
    if not idx:
        return None
    word_at = {}
    for word, positions in idx.items():
        for position in positions:
            word_at[position] = word
    return " ".join(word_at[position] for position in sorted(word_at))


def inverted_index_to_text_lenient(idx) -> str | None:
    """Like :func:`inverted_index_to_text`, but tolerant of malformed input.

    ``None`` -> ``None``; a non-dict, an empty dict or one without valid
    integer positions -> ``""``; non-list position entries and non-int
    positions are skipped.  Formerly ``new_dl.convert_inverted_index_to_text``.
    """
    if idx is None:
        return None
    if not isinstance(idx, dict):
        return ""
    pos_terms: list[tuple[int, str]] = []
    for term, positions in idx.items():
        if not isinstance(positions, list):
            continue
        for pos in positions:
            if isinstance(pos, int):
                pos_terms.append((pos, term))
    if not pos_terms:
        return ""
    pos_terms.sort(key=lambda pair: pair[0])
    return " ".join(term for _, term in pos_terms)


def openalex_work_to_record(item: dict, *, run_id=None) -> dict:
    """Map an OpenAlex work into a search-result/queue record (``ingest_source='keyword_search'``)."""
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


_CROSSREF_DATE_FIELDS = ("published-print", "published-online", "published")


def crossref_item_to_openalex_like(item: dict) -> dict:
    """Map a Crossref ``/works`` item into the OpenAlex-like shape the matcher scores.

    The year comes from the first of ``published-print``, ``published-online``,
    ``published`` that has one (``issued`` is not read).  Formerly two copies in
    ``OpenAlexScraper``: step 8 of ``search()`` (this superset) and
    ``search_crossref`` (``published-print`` only, without the four trailing keys).
    An item with an empty ``title`` list still raises ``IndexError``, as both did.
    """
    pub_year = None
    for date_field in _CROSSREF_DATE_FIELDS:
        if date_parts := item.get(date_field, {}).get("date-parts", [[None]])[0]:
            pub_year = date_parts[0]
            if pub_year:
                break

    return {
        "id": f"crossref:{item.get('DOI', '')}",
        "doi": item.get("DOI"),
        "display_name": item.get("title", [""])[0],
        "publication_year": pub_year,
        "type": item.get("type"),
        "authorships": [
            {"author": {"display_name": f"{author.get('given', '')} {author.get('family', '')}"}}
            for author in item.get("author", [])
        ],
        "referenced_works": [],  # Crossref doesn't provide this directly
        "abstract_inverted_index": None,  # Crossref doesn't provide this
        "keywords": [],  # Crossref doesn't provide this
        "cited_by_api_url": item.get("is-referenced-by-count", 0) > 0
        and f"https://api.crossref.org/works/{item.get('DOI')}/references"
        or None,
    }
