# Pflichtenheft – Korpus-Builder 2.0 (dl_lit_project)

## Projektziel

Erweiterung und Produktionsreife des bestehenden Repositories `dl_lit_project` zu einem webbasierten, interaktiven Korpus-Builder zur automatisierten Erstellung wissenschaftlicher Referenzkorpora.

---

## Muss-Funktionen

### M-01: Code-Audit & Refactoring
- Sichtung und Bereinigung bestehender Module (`get_bib_pages.py`, `APIscraper_v2.py`, `OpenAlexScraper.py`, `new_dl.py`, `cli.py`)
- Konsolidierung von Datenbanktabellen und CLI-Schnittstellen

### M-02: Keyword-Suche
- Komplexe Suche mit logischen Operatoren (AND/OR)
- Iteratives Quellen-Einlesen: „Quellen von Quellen“ (rekursiv)
- Ergebnispersistenz in Datenbank
- Input: freie Textsuche oder JSON-Seed

### M-03: Visualisierung
- D3.js-Komponente zur Darstellung von Korpusstruktur und Zitationsnetzwerken
- Knoten = Referenzen, Kanten = Zitierbeziehungen
- Farbliche Clustering-Optionen (Themen / Suchpfade)

### M-04: Frontend (React + Vite)
- Upload-Funktion für Seed-Dokumente (PDF, BibTeX, JSON)
- Keyword-Suchmaske mit Optionen (Tiefe, Filter etc.)
- Anzeige des Korpusgraphen und Fortschrittsbalken
- Interface zum Download der finalen Referenzdaten (BibTeX, JSON, PDFs als ZIP)

### M-05: Dockerisierung
- `docker-compose.yml` mit persistenter SQLite-Datenbank
- Trennung in Web-Frontend, Backend-API und Downloader
- Script zur Ersteinrichtung mit Umgebungsvariablen

### M-06: Dokumentation
- README mit Installationsanleitung
- User-Doku (markdown oder PDF)
- Entwickler-Doku zur Erweiterbarkeit

---

## Qualitätsanforderungen

- Python-Quellcode entspricht gängigen Konventionen (z. B. PEP8)
- Einsatz automatischer Linter (z. B. `ruff`) zur Grobprüfung
- React-Frontend nutzt komponentenbasierte Struktur, Tailwind für Styles
- Keine Performance- oder Usability-Vorgaben

---

## Abgrenzung

Nicht Bestandteil des Projekts:
- Hosting oder Serverbetrieb (wird vom AG bereitgestellt)
- Unterstützung proprietärer Datenbanken (z. B. Scopus, Web of Science)
- OCR-Optimierung für ungewöhnliche Dokumentformate

---

## Technische Abhängigkeiten

- OpenAlex API, Crossref API
- Google Gemini API (OCR & Parsing)
- React, Vite, TailwindCSS
- D3.js, Docker, SQLite3
