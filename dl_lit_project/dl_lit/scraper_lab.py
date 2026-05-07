"""Isolated scraper execution for Scraper Lab.

This module intentionally writes only to scraper_* tables and artifact files.
It does not insert into works/corpus_works or the bibliography pipeline.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


load_dotenv()


SCRAPER_SOURCES = {"eurlex", "expert_groups"}


def utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def safe_filename(value: str | None, fallback: str = "artifact") -> str:
    text = str(value or "").strip()
    text = re.sub(r"[^\w.\-]+", "_", text, flags=re.UNICODE)
    text = text.strip("._")
    return text[:160] or fallback


def stable_hash(value: object, length: int = 12) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False) if not isinstance(value, str) else value
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def year_from_date(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\b(19|20)\d{2}\b", str(value))
    return int(match.group(0)) if match else None


def raise_for_eurlex_response(content: bytes, status_code: int) -> None:
    fault_message = ""
    try:
        root = ElementTree.fromstring(content)
        fault = root.find(".//{http://www.w3.org/2003/05/soap-envelope}Fault")
        if fault is not None:
            text = fault.find(".//{http://www.w3.org/2003/05/soap-envelope}Text")
            fault_message = (text.text or "").strip() if text is not None else ""
    except ElementTree.ParseError:
        fault_message = ""

    if fault_message:
        raise RuntimeError(f"EUR-Lex SOAP fault: {fault_message}")
    if int(status_code or 0) >= 400:
        raise RuntimeError(f"EUR-Lex request failed with HTTP {status_code}")


def ensure_scraper_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS scraper_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_type TEXT NOT NULL,
          query_json TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          requested_by_user_id INTEGER,
          total_count INTEGER NOT NULL DEFAULT 0,
          downloaded_count INTEGER NOT NULL DEFAULT 0,
          failed_count INTEGER NOT NULL DEFAULT 0,
          error TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          started_at TIMESTAMP,
          finished_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS scraper_artifacts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER NOT NULL,
          source_type TEXT NOT NULL,
          source_id TEXT,
          title TEXT,
          date TEXT,
          year INTEGER,
          source_url TEXT,
          download_url TEXT,
          local_artifact_path TEXT,
          file_name TEXT,
          raw_metadata_json TEXT,
          bibtex_key TEXT,
          bibtex_entry TEXT,
          metadata_completeness TEXT,
          status TEXT NOT NULL DEFAULT 'pending',
          error TEXT,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_scraper_artifacts_run ON scraper_artifacts(run_id, id);
        CREATE INDEX IF NOT EXISTS idx_scraper_runs_created ON scraper_runs(created_at DESC, id DESC);
        """
    )
    conn.commit()


def build_eurlex_expert_query(form_data: dict) -> str:
    parts: list[str] = []
    if str(form_data.get("fm_coded", "")).strip():
        parts.append(f"FM_CODED = {str(form_data['fm_coded']).strip()}")
    if str(form_data.get("dd_year", "")).strip():
        parts.append(f"DD_YEAR = {str(form_data['dd_year']).strip()}")
    if str(form_data.get("celex_id", "")).strip():
        parts.append(f"DN = {str(form_data['celex_id']).strip()}")
    if str(form_data.get("lang", "")).strip():
        parts.append(f"AU = {str(form_data['lang']).strip()}")
    if str(form_data.get("text_query", "")).strip():
        text = str(form_data["text_query"]).strip().replace("'", "''")
        parts.append(f"TI ~ '{text}'")
    return " AND ".join(parts)


def scrape_eurlex(query: dict) -> list[dict]:
    from lxml import etree
    from requests import Session
    from zeep import Client
    from zeep.cache import SqliteCache
    from zeep.transports import Transport
    from zeep.wsse.username import UsernameToken

    username = os.getenv("EURLEX_USERNAME")
    password = os.getenv("EURLEX_PASSWORD")
    if not username or not password:
        raise RuntimeError("Missing EURLEX_USERNAME/EURLEX_PASSWORD")

    expert_query = str(query.get("expert_query") or "").strip() or build_eurlex_expert_query(query)
    if not expert_query:
        raise ValueError("EUR-Lex query must include at least one search field")

    session = Session()
    transport = Transport(session=session, cache=SqliteCache(), timeout=20)
    client = Client(
        "https://eur-lex.europa.eu/EURLexWebService?wsdl",
        transport=transport,
        wsse=UsernameToken(username, password),
    )

    page = 1
    results: list[dict] = []
    while True:
        with client.settings(raw_response=True):
            response = client.service.doQuery(
                expertQuery=expert_query,
                page=page,
                pageSize=100,
                searchLanguage="en",
            )
        raise_for_eurlex_response(response.content, response.status_code)
        root = etree.fromstring(response.content)
        rows = root.xpath("//*[local-name()='result']")
        if not rows:
            break
        for row in rows:
            celex = row.xpath(".//*[local-name()='ID_CELEX']/*[local-name()='VALUE']/text()")
            title = row.xpath(".//*[local-name()='EXPRESSION_TITLE']/*[local-name()='VALUE']/text()")
            date = row.xpath(".//*[local-name()='WORK_DATE_DOCUMENT']/*[local-name()='VALUE']/text()")
            pdf = row.xpath(".//*[local-name()='document_link'][@type='pdf']/text()")
            html = row.xpath(".//*[local-name()='document_link'][@type='html']/text()")
            results.append(
                {
                    "celex": celex[0] if celex else None,
                    "title": title[0] if title else None,
                    "date": date[0] if date else None,
                    "pdf": pdf[0] if pdf else None,
                    "html": html[0] if html else None,
                    "expert_query": expert_query,
                }
            )
        if len(rows) < 100:
            break
        page += 1
    return results


def eurlex_download_info(metadata: dict, index: int) -> tuple[str, str | None, str]:
    source_id = metadata.get("celex") or f"no_celex_{index}"
    if metadata.get("pdf"):
        return str(source_id), str(metadata["pdf"]), ".pdf"
    if metadata.get("html"):
        return str(source_id), str(metadata["html"]), ".html"
    if metadata.get("celex"):
        return str(source_id), f"https://eur-lex.europa.eu/legal-content/ALL/?uri=CELEX:{metadata['celex']}", ".html"
    return str(source_id), None, ".html"


def scrape_expert_groups(query: dict) -> list[dict]:
    from playwright.sync_api import sync_playwright

    group_id = str(query.get("group_id") or query.get("query") or "").strip()
    if not group_id:
        raise ValueError("Expert Groups query must include group_id")

    base_url = "https://ec.europa.eu"
    url = f"{base_url}/transparency/expert-groups-register/screen/expert-groups/consult?lang=en&groupID={group_id}"
    meetings: list[dict] = []
    seen: set[str] = set()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(4000)
        soup = BeautifulSoup(page.content(), "html.parser")
        title_el = soup.select_one("div.ecl-page-header-harmonised__title")
        group_title = title_el.get_text(" ", strip=True) if title_el else group_id

        while True:
            soup = BeautifulSoup(page.content(), "html.parser")
            table = soup.select_one("app-expert-groups-meetings table") or soup.select_one("table")
            if not table:
                break
            rows = table.select("tbody tr")
            if not rows:
                break
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                link = cells[1].find("a")
                if not link:
                    continue
                full_url = urljoin(base_url, link.get("href", ""))
                if full_url in seen:
                    continue
                seen.add(full_url)
                meetings.append(
                    {
                        "expert_group_id": group_id,
                        "group_title": group_title,
                        "date": cells[0].get_text(" ", strip=True),
                        "title": link.get_text(" ", strip=True),
                        "meeting_url": full_url,
                    }
                )

            current = soup.select_one("ecl-pagination-item.ecl-pagination__item--current .ecl-pagination__text--summary")
            if not current:
                break
            try:
                next_page = str(int(current.get_text(strip=True)) + 1)
            except Exception:
                break
            rows_locator = page.locator("tbody tr")
            if rows_locator.count() == 0:
                break
            old_first = rows_locator.first.inner_text()
            clicked = page.evaluate(
                """
                nextPage => {
                  const items = Array.from(document.querySelectorAll('ecl-pagination-item.ecl-pagination__item'));
                  for (const item of items) {
                    if (item.classList.contains('ecl-pagination__item--current')) continue;
                    const a = item.querySelector('a');
                    if (a && a.innerText.trim() === nextPage) {
                      a.click();
                      return true;
                    }
                  }
                  return false;
                }
                """,
                next_page,
            )
            if not clicked:
                break
            try:
                page.wait_for_function(
                    """oldText => {
                      const row = document.querySelector('tbody tr');
                      return row && row.innerText !== oldText;
                    }""",
                    arg=old_first,
                    timeout=5000,
                )
            except Exception:
                break

        artifacts: list[dict] = []
        for meeting in meetings:
            page.goto(meeting["meeting_url"], wait_until="networkidle")
            page.wait_for_timeout(4000)
            soup = BeautifulSoup(page.content(), "html.parser")
            for link in soup.select("a[href]"):
                href = link.get("href")
                if not href or ("document" not in href and not href.lower().split("?")[0].endswith(".pdf")):
                    continue
                doc_url = urljoin(base_url, href)
                doc_title = link.get_text(" ", strip=True)
                artifacts.append(
                    {
                        **meeting,
                        "doc_url": doc_url,
                        "doc_title": doc_title,
                    }
                )
        browser.close()
    return artifacts


def validate_download_content(content: bytes, path: Path) -> None:
    if not content:
        raise RuntimeError("Downloaded artifact is empty")
    if path.suffix.lower() == ".pdf" and not content.lstrip().startswith(b"%PDF-"):
        raise RuntimeError("Downloaded artifact is not a valid PDF")


def download_file(url: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    validate_download_content(response.content, path)
    path.write_bytes(response.content)


def build_artifact_record(source_type: str, metadata: dict, index: int, run_dir: Path) -> dict:
    if source_type == "eurlex":
        source_id, download_url, ext = eurlex_download_info(metadata, index)
        key = f"eurlex_{safe_filename(source_id, stable_hash(metadata))}"
        title = metadata.get("title") or f"EUR-Lex document {source_id}"
        source_url = metadata.get("html") or (f"https://eur-lex.europa.eu/legal-content/ALL/?uri=CELEX:{source_id}" if metadata.get("celex") else download_url)
        date = metadata.get("date")
    else:
        source_id = str(metadata.get("doc_url") or stable_hash(metadata))
        download_url = metadata.get("doc_url")
        ext = os.path.splitext(str(download_url or "").split("?", 1)[0])[1] or ".pdf"
        group_id = safe_filename(str(metadata.get("expert_group_id") or "group"))
        date_slug = safe_filename(str(metadata.get("date") or "date"))
        key = f"expertgroups_{group_id}_{date_slug}_{stable_hash(source_id, 8)}"
        title = metadata.get("doc_title") or metadata.get("title") or "Untitled expert group document"
        source_url = metadata.get("meeting_url") or download_url
        date = metadata.get("date")

    file_name = safe_filename(key) + ext
    local_path = run_dir / file_name
    completeness = "minimal" if title and date and download_url else "partial"
    return {
        "source_id": source_id,
        "title": title,
        "date": date,
        "year": year_from_date(date),
        "source_url": source_url,
        "download_url": download_url,
        "local_artifact_path": str(local_path),
        "file_name": file_name,
        "raw_metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
        "bibtex_key": key,
        "bibtex_entry": "",
        "metadata_completeness": completeness,
    }


def execute_scraper_job(db_path: str, run_id: int, artifacts_root: str) -> dict:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    ensure_scraper_schema(conn)
    row = conn.execute("SELECT * FROM scraper_runs WHERE id = ?", (run_id,)).fetchone()
    if not row:
        raise ValueError(f"Unknown scraper run id: {run_id}")

    source_type = str(row["source_type"] or "").strip()
    if source_type not in SCRAPER_SOURCES:
        raise ValueError(f"Unsupported scraper source: {source_type}")
    query = json.loads(row["query_json"] or "{}")
    run_dir = Path(artifacts_root) / "scraper_lab" / f"run_{run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    conn.execute(
        "UPDATE scraper_runs SET status = 'running', started_at = CURRENT_TIMESTAMP, error = NULL WHERE id = ?",
        (run_id,),
    )
    conn.commit()

    try:
        raw_results = scrape_eurlex(query) if source_type == "eurlex" else scrape_expert_groups(query)
        downloaded = 0
        failed = 0
        for index, metadata in enumerate(raw_results, start=1):
            record = build_artifact_record(source_type, metadata, index, run_dir)
            status = "downloaded"
            error = None
            if record["download_url"]:
                try:
                    download_file(record["download_url"], Path(record["local_artifact_path"]))
                    Path(record["local_artifact_path"]).with_suffix(".json").write_text(
                        record["raw_metadata_json"],
                        encoding="utf-8",
                    )
                    downloaded += 1
                except Exception as exc:
                    status = "failed"
                    error = str(exc)
                    failed += 1
            else:
                status = "failed"
                error = "No download URL found"
                failed += 1

            conn.execute(
                """
                INSERT INTO scraper_artifacts (
                  run_id, source_type, source_id, title, date, year, source_url,
                  download_url, local_artifact_path, file_name, raw_metadata_json,
                  bibtex_key, bibtex_entry, metadata_completeness, status, error,
                  created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (
                    run_id,
                    source_type,
                    record["source_id"],
                    record["title"],
                    record["date"],
                    record["year"],
                    record["source_url"],
                    record["download_url"],
                    record["local_artifact_path"],
                    record["file_name"],
                    record["raw_metadata_json"],
                    record["bibtex_key"],
                    record["bibtex_entry"],
                    record["metadata_completeness"],
                    status,
                    error,
                ),
            )
            conn.commit()

        conn.execute(
            """
            UPDATE scraper_runs
               SET status = 'completed',
                   total_count = ?,
                   downloaded_count = ?,
                   failed_count = ?,
                   finished_at = CURRENT_TIMESTAMP
             WHERE id = ?
            """,
            (len(raw_results), downloaded, failed, run_id),
        )
        conn.commit()
        return {"run_id": run_id, "source_type": source_type, "total": len(raw_results), "downloaded": downloaded, "failed": failed}
    except Exception as exc:
        conn.execute(
            """
            UPDATE scraper_runs
               SET status = 'failed',
                   error = ?,
                   finished_at = CURRENT_TIMESTAMP
             WHERE id = ?
            """,
            (str(exc), run_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()
