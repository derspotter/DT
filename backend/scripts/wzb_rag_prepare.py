#!/usr/bin/env python3
"""Prepare Weizenbaum abstract ODT metadata for the Kantropos RAG updater.

The Kantropos corpus-updater currently discovers documents by listing ``*.pdf``
files in a corpus directory, but it reuses matching ``*.txt`` sidecars when
present. This tool stages text sidecars from WZB ODT files and creates small
PDF placeholders so the current updater can embed the extracted text without
LibreOffice or Pandoc.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from xml.etree import ElementTree as ET
from zipfile import ZipFile


DEFAULT_SOURCE_DIR = "/home/spott/wzb"
DEFAULT_BIB_NAME = "metadata_wzb_abstracts.bib"
DEFAULT_OUTPUT_DIR = "/tmp/wzb_rag_drafts"
DEFAULT_TARGET_NAME = "Weizenbaum Abstracts"
DEFAULT_CORPORA_ROOT = "/data/projects/kantropos/corpora"
PDF_PLACEHOLDER = b"""%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length 100 >>
stream
BT /F1 12 Tf 72 720 Td (Text-only RAG placeholder. See matching .txt sidecar.) Tj ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
xref
0 6
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000236 00000 n 
0000000386 00000 n 
trailer
<< /Size 6 /Root 1 0 R >>
startxref
456
%%EOF
"""


@dataclass
class BibEntry:
    entry_type: str
    key: str
    fields: dict[str, str]
    raw: str


def timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def slugify(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return text or "wzb"


def normalize_path_key(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "")
    text = text.replace("\\", "/")
    text = re.sub(r"\s+", " ", text).strip()
    return text.casefold()


def strip_outer_braces(value: str) -> str:
    text = (value or "").strip()
    while len(text) >= 2 and text[0] == "{" and text[-1] == "}":
        depth = 0
        balanced_outer = True
        for idx, ch in enumerate(text):
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0 and idx != len(text) - 1:
                    balanced_outer = False
                    break
        if not balanced_outer:
            break
        text = text[1:-1].strip()
    return text


def split_top_level_fields(body: str) -> list[str]:
    fields: list[str] = []
    start = 0
    depth = 0
    quote = False
    escape = False
    for idx, ch in enumerate(body):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"' and depth == 0:
            quote = not quote
            continue
        if quote:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth = max(0, depth - 1)
        elif ch == "," and depth == 0:
            part = body[start:idx].strip()
            if part:
                fields.append(part)
            start = idx + 1
    tail = body[start:].strip()
    if tail:
        fields.append(tail)
    return fields


def parse_bibtex(text: str) -> list[BibEntry]:
    entries: list[BibEntry] = []
    idx = 0
    while True:
        match = re.search(r"@([A-Za-z]+)\s*\{", text[idx:])
        if not match:
            break
        entry_start = idx + match.start()
        body_start = idx + match.end()
        depth = 1
        pos = body_start
        escape = False
        while pos < len(text) and depth > 0:
            ch = text[pos]
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
            pos += 1
        if depth != 0:
            raise ValueError(f"Unbalanced BibTeX entry near byte {entry_start}")
        raw = text[entry_start:pos]
        body = text[body_start:pos - 1]
        if "," not in body:
            raise ValueError(f"Missing BibTeX key near byte {entry_start}")
        key, field_body = body.split(",", 1)
        fields: dict[str, str] = {}
        for part in split_top_level_fields(field_body):
            if "=" not in part:
                continue
            name, value = part.split("=", 1)
            fields[name.strip().lower()] = strip_outer_braces(value.strip().rstrip(","))
        entries.append(BibEntry(match.group(1).lower(), key.strip(), fields, raw))
        idx = pos
    return entries


def bibtex_escape(value: object) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\\", "\\textbackslash{}")
    text = text.replace("{", "\\{").replace("}", "\\}")
    return re.sub(r"\s+", " ", text).strip()


def render_bibtex(entries: list[BibEntry], file_names: dict[str, str]) -> str:
    rendered: list[str] = []
    preferred = ["title", "author", "year", "abstract", "doi", "journal", "booktitle", "url"]
    for entry in entries:
        fields = dict(entry.fields)
        fields["file"] = f"{file_names[entry.key]}:PDF"
        ordered = [name for name in preferred if fields.get(name)]
        ordered.extend(name for name in sorted(fields) if name not in ordered and name != "file")
        ordered.append("file")
        lines = [f"@{entry.entry_type}{{{entry.key},"]
        for idx, name in enumerate(ordered):
            comma = "," if idx < len(ordered) - 1 else ""
            lines.append(f"  {name} = {{{{{bibtex_escape(fields[name])}}}}}{comma}")
        lines.append("}")
        rendered.append("\n".join(lines))
    return "\n\n".join(rendered) + "\n"


def parse_file_field(value: str) -> tuple[str, str]:
    raw = strip_outer_braces(value)
    first = raw.split(";", 1)[0].strip()
    parts = first.split(":")
    if len(parts) >= 3 and not parts[0]:
        return parts[1].strip(), parts[2].strip()
    if len(parts) >= 3:
        return parts[1].strip(), parts[-1].strip()
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    return first, ""


def index_source_files(source_dir: Path) -> dict[str, Path]:
    index: dict[str, Path] = {}
    for path in source_dir.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(source_dir).as_posix()
        index[normalize_path_key(rel)] = path
    return index


def resolve_source_file(source_dir: Path, source_index: dict[str, Path], rel_path: str) -> Path | None:
    direct = source_dir / rel_path
    if direct.exists():
        return direct
    normalized = normalize_path_key(rel_path)
    if normalized in source_index:
        return source_index[normalized]
    basename_key = normalize_path_key(Path(rel_path).name)
    matches = [path for key, path in source_index.items() if key.endswith("/" + basename_key) or key == basename_key]
    if len(matches) == 1:
        return matches[0]
    compact = re.sub(r"[\s._-]+", "", normalized)
    compact_matches = [
        path for key, path in source_index.items()
        if re.sub(r"[\s._-]+", "", key) == compact
    ]
    if len(compact_matches) == 1:
        return compact_matches[0]
    return None


def text_from_odt(path: Path) -> str:
    with ZipFile(path) as archive:
        xml = archive.read("content.xml")
    root = ET.fromstring(xml)
    paragraphs: list[str] = []
    paragraph_tags = {
        "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}p",
        "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}h",
    }
    line_break_tag = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}line-break"
    tab_tag = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}tab"

    def collect(element: ET.Element) -> str:
        parts: list[str] = []
        if element.text:
            parts.append(element.text)
        for child in list(element):
            if child.tag == line_break_tag:
                parts.append("\n")
            elif child.tag == tab_tag:
                parts.append("\t")
            else:
                parts.append(collect(child))
            if child.tail:
                parts.append(child.tail)
        return "".join(parts)

    for element in root.iter():
        if element.tag in paragraph_tags:
            paragraph = re.sub(r"[ \t]+", " ", collect(element)).strip()
            if paragraph:
                paragraphs.append(paragraph)
    return "\n\n".join(paragraphs).strip()


def sidecar_text(entry: BibEntry, source_path: Path | None, rel_path: str) -> tuple[str, str]:
    extracted = ""
    extraction_status = "metadata_only"
    if source_path and source_path.suffix.lower() == ".odt":
        try:
            extracted = text_from_odt(source_path)
            extraction_status = "odt_extracted"
        except Exception as exc:  # noqa: BLE001 - keep batch conversion moving.
            extracted = f"[ODT extraction failed: {exc}]"
            extraction_status = "odt_failed"
    elif source_path and source_path.suffix.lower() == ".txt":
        extracted = source_path.read_text(encoding="utf-8", errors="replace").strip()
        extraction_status = "txt_copied"
    elif source_path and source_path.suffix.lower() == ".pdf":
        extraction_status = "pdf_metadata_only"

    title = entry.fields.get("title", "").strip()
    author = entry.fields.get("author", "").strip()
    year = entry.fields.get("year", "").strip()
    abstract = entry.fields.get("abstract", "").strip()
    chunks = []
    if title:
        chunks.append(f"# {title}")
    metadata_lines = []
    if author:
        metadata_lines.append(f"Authors: {author}")
    if year:
        metadata_lines.append(f"Year: {year}")
    if rel_path:
        metadata_lines.append(f"Source file: {rel_path}")
    if metadata_lines:
        chunks.append("\n".join(metadata_lines))
    abstract_norm = re.sub(r"\W+", "", abstract).casefold()
    extracted_norm = re.sub(r"\W+", "", extracted).casefold()
    if abstract and (not extracted_norm or abstract_norm[:300] not in extracted_norm):
        chunks.append(f"## Abstract\n\n{abstract}")
    if extracted and extracted != abstract:
        chunks.append(f"## Extracted Document Text\n\n{extracted}")
    return "\n\n".join(chunks).strip() + "\n", extraction_status


def draft(args: argparse.Namespace) -> dict:
    source_dir = Path(args.source_dir).expanduser().resolve()
    bib_path = Path(args.bib) if args.bib else source_dir / DEFAULT_BIB_NAME
    if not bib_path.exists():
        raise SystemExit(f"Missing BibTeX file: {bib_path}")
    entries = parse_bibtex(bib_path.read_text(encoding="utf-8"))
    if not entries:
        raise SystemExit(f"No BibTeX entries found in {bib_path}")

    draft_dir = Path(args.output_dir).expanduser().resolve() / f"wzb_{timestamp()}"
    files_dir = draft_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=False)

    source_index = index_source_files(source_dir)
    used_names: set[str] = set()
    file_names: dict[str, str] = {}
    manifest = []
    errors = []

    for entry in entries:
        file_value = entry.fields.get("file", "")
        rel_path, file_kind = parse_file_field(file_value)
        source_path = resolve_source_file(source_dir, source_index, rel_path) if rel_path else None
        if not entry.fields.get("title"):
            errors.append(f"{entry.key}: missing title")
        if not entry.fields.get("author"):
            errors.append(f"{entry.key}: missing author")
        if not entry.fields.get("year"):
            errors.append(f"{entry.key}: missing year")
        if not entry.fields.get("abstract"):
            errors.append(f"{entry.key}: missing abstract")

        base = slugify(entry.key)
        pdf_name = f"{base}.pdf"
        counter = 2
        while pdf_name in used_names:
            pdf_name = f"{base}_{counter}.pdf"
            counter += 1
        used_names.add(pdf_name)
        file_names[entry.key] = pdf_name

        text_name = f"{Path(pdf_name).stem}.txt"
        text, extraction_status = sidecar_text(entry, source_path, rel_path)
        (files_dir / text_name).write_text(text, encoding="utf-8")
        if source_path and source_path.suffix.lower() == ".pdf" and args.copy_pdfs:
            shutil.copy2(source_path, files_dir / pdf_name)
            pdf_status = "copied_pdf"
        else:
            (files_dir / pdf_name).write_bytes(PDF_PLACEHOLDER)
            pdf_status = "placeholder_pdf"

        manifest.append({
            "bibtex_key": entry.key,
            "source_file": str(source_path) if source_path else "",
            "source_file_reference": rel_path,
            "source_file_kind": file_kind,
            "source_missing": source_path is None,
            "target_pdf": pdf_name,
            "target_text": text_name,
            "extraction_status": extraction_status,
            "pdf_status": pdf_status,
            "text_chars": len(text),
        })

    missing = [item for item in manifest if item["source_missing"]]
    if missing and not args.allow_missing_sources:
        sample = ", ".join(item["source_file_reference"] for item in missing[:5])
        raise SystemExit(f"{len(missing)} source files could not be resolved. Sample: {sample}")

    metadata = render_bibtex(entries, file_names)
    (draft_dir / "metadata.bib").write_text(metadata, encoding="utf-8")
    (draft_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    (draft_dir / "README.txt").write_text(
        "\n".join([
            "WZB RAG draft",
            "",
            "Copy metadata.bib and files/* into the target Kantropos corpus directory.",
            "The .pdf files are placeholders when the original source was an ODT.",
            "The .txt files contain the text that Kantropos will copy into markdown/ and embed.",
            "",
        ]),
        encoding="utf-8",
    )
    return {
        "draft_dir": str(draft_dir),
        "entries": len(entries),
        "missing_sources": len(missing),
        "metadata_bib": str(draft_dir / "metadata.bib"),
        "files_dir": str(files_dir),
        "errors": errors,
    }


def validate_draft(draft_dir: Path) -> dict:
    metadata_path = draft_dir / "metadata.bib"
    manifest_path = draft_dir / "manifest.json"
    files_dir = draft_dir / "files"
    errors: list[str] = []
    if not metadata_path.exists():
        errors.append(f"missing {metadata_path}")
        entries: list[BibEntry] = []
    else:
        entries = parse_bibtex(metadata_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else []
    file_values = {entry.key: parse_file_field(entry.fields.get("file", ""))[0] for entry in entries}
    for item in manifest:
        key = item.get("bibtex_key")
        pdf_name = item.get("target_pdf")
        text_name = item.get("target_text")
        if file_values.get(key) != pdf_name:
            errors.append(f"{key}: metadata file field does not match {pdf_name}")
        if not (files_dir / str(pdf_name)).exists():
            errors.append(f"{key}: missing staged PDF {pdf_name}")
        if not (files_dir / str(text_name)).exists():
            errors.append(f"{key}: missing staged text {text_name}")
    return {
        "draft_dir": str(draft_dir),
        "entries": len(entries),
        "manifest_entries": len(manifest),
        "errors": errors,
        "ok": not errors,
    }


def apply_draft(args: argparse.Namespace) -> dict:
    draft_dir = Path(args.draft_dir).expanduser().resolve()
    validation = validate_draft(draft_dir)
    if not validation["ok"]:
        raise SystemExit(json.dumps(validation, indent=2, ensure_ascii=False))
    target_dir = Path(args.target_dir or (Path(args.corpora_root) / args.target_name)).expanduser().resolve()
    metadata_src = draft_dir / "metadata.bib"
    files_dir = draft_dir / "files"
    actions = []
    target_metadata = target_dir / "metadata.bib"
    if target_metadata.exists():
        actions.append(("backup", str(target_metadata), str(target_dir / f"metadata.bib.{timestamp()}.bak")))
    actions.append(("copy_metadata", str(metadata_src), str(target_metadata)))
    for path in sorted(files_dir.iterdir()):
        if path.is_file():
            actions.append(("copy_file", str(path), str(target_dir / path.name)))

    if not args.yes:
        return {
            "dry_run": True,
            "target_dir": str(target_dir),
            "actions": actions,
            "message": "Rerun with --yes to apply.",
        }

    target_dir.mkdir(parents=True, exist_ok=True)
    for action, src, dst in actions:
        if action == "backup":
            shutil.copy2(src, dst)
        elif action in {"copy_metadata", "copy_file"}:
            shutil.copy2(src, dst)
    return {
        "dry_run": False,
        "target_dir": str(target_dir),
        "actions": len(actions),
        "metadata_bib": str(target_metadata),
    }


def commands(args: argparse.Namespace) -> dict:
    corpus = args.target_name
    encoded = quote(corpus, safe="")
    return {
        "corpus": corpus,
        "commands": [
            f'docker exec kantropos-corpus-updater curl -fsS -X POST "http://localhost:8001/markdowns/{encoded}"',
            f'docker exec kantropos-corpus-updater curl -fsS -X POST "http://localhost:8001/embeddings/{encoded}?sync_mode=INSERT"',
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--bib", default="")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--target-name", default=DEFAULT_TARGET_NAME)
    parser.add_argument("--target-dir", default="")
    parser.add_argument("--corpora-root", default=DEFAULT_CORPORA_ROOT)
    sub = parser.add_subparsers(dest="cmd", required=True)

    draft_parser = sub.add_parser("draft", help="Create a RAG-ready WZB draft.")
    draft_parser.add_argument("--allow-missing-sources", action="store_true")
    draft_parser.add_argument("--copy-pdfs", action="store_true", help="Copy real source PDFs when entries reference PDFs.")

    validate_parser = sub.add_parser("validate", help="Validate a draft.")
    validate_parser.add_argument("--draft-dir", required=True)

    apply_parser = sub.add_parser("apply", help="Apply a draft to a Kantropos corpus directory.")
    apply_parser.add_argument("--draft-dir", required=True)
    apply_parser.add_argument("--yes", action="store_true")

    sub.add_parser("commands", help="Print Kantropos markdown and embedding commands.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.cmd == "draft":
            payload = draft(args)
        elif args.cmd == "validate":
            payload = validate_draft(Path(args.draft_dir).expanduser().resolve())
        elif args.cmd == "apply":
            payload = apply_draft(args)
        elif args.cmd == "commands":
            payload = commands(args)
        else:
            raise SystemExit(f"Unknown command: {args.cmd}")
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI should return JSON on unexpected failures.
        print(json.dumps({"error": str(exc)}, indent=2, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
