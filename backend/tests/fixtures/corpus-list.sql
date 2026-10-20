-- Fixture for tests/corpus-list.test.js: corpus 1 holds works 1-14, works
-- 15-17 sit outside it. Covers every status_from_row branch, each authors
-- shape, odd years, a NULL membership date, LIKE metacharacters in titles and
-- every provenance path of source_label_for_work. Work 12 has a leading BOM
-- in its first author, which Python's str.strip() keeps and String#trim drops.
-- Work 9's metadata_status is wrapped in U+00A0/U+3000 (str.strip() removes
-- them) and work 13's year is Arabic-Indic digits (int() reads them as 1970).
CREATE TABLE works (
  id INTEGER PRIMARY KEY,
  title TEXT,
  authors TEXT,
  year INTEGER,
  doi TEXT,
  openalex_id TEXT,
  source TEXT,
  publisher TEXT,
  type TEXT,
  metadata_status TEXT,
  metadata_source TEXT,
  download_status TEXT,
  file_path TEXT,
  origin_type TEXT,
  origin_key TEXT,
  source_pdf TEXT,
  run_id INTEGER,
  created_at TIMESTAMP
);
CREATE TABLE corpus_works (corpus_id INTEGER NOT NULL, work_id INTEGER NOT NULL, added_at TIMESTAMP, PRIMARY KEY (corpus_id, work_id));
CREATE TABLE search_runs (id INTEGER PRIMARY KEY, query TEXT);
CREATE TABLE ingest_source_metadata (corpus_id INTEGER, ingest_source TEXT, title TEXT, authors TEXT, source_pdf TEXT);
CREATE TABLE seed_candidates_in_corpus (corpus_id INTEGER, work_id INTEGER, source_type TEXT, source_key TEXT, marked_at TEXT);

INSERT INTO works (id, title, authors, year, doi, openalex_id, source, publisher, type, metadata_status, metadata_source, download_status, file_path, origin_type, origin_key, source_pdf, run_id, created_at) VALUES
  (1,  'Labour markets and 100% employment', '["Smith, J", "Doe, A"]', 2001, '10.1/one', 'W1', 'Journal of Labour', 'Pub A', 'article', 'matched', 'openalex', 'not_requested', NULL, NULL, NULL, NULL, NULL, '2025-01-01 00:00:01'),
  (2,  'The a_b test', 'Plain Author', 1999, NULL, 'W2', 'Econ Letters', NULL, 'article', 'matched', NULL, 'downloaded', '/pdfs/2.pdf', NULL, NULL, NULL, NULL, '2025-01-01 00:00:02'),
  (3,  'Aab testing', '[]', NULL, NULL, NULL, NULL, NULL, NULL, 'pending', NULL, 'queued', NULL, 'search', 'search:7', NULL, NULL, '2025-01-01 00:00:03'),
  (4,  'Zeta Economics', NULL, 'n.d.', NULL, NULL, 'Zeta Review', NULL, NULL, 'in_progress', NULL, 'not_requested', NULL, NULL, 'search:99', NULL, NULL, '2025-01-01 00:00:04'),
  (5,  'beta 100 percent', '["", "  ", "Ångström, K"]', 2010, NULL, NULL, 'Physics', NULL, NULL, 'failed', NULL, 'not_requested', NULL, NULL, NULL, NULL, 7, '2025-01-01 00:00:05'),
  (6,  'Éclair studies', '["Émile"]', 2010, NULL, NULL, NULL, NULL, NULL, ' MATCHED ', NULL, 'In_Progress', NULL, NULL, NULL, NULL, 42, '2025-01-01 00:00:06'),
  (7,  'labour history', 'labour', 2005, NULL, NULL, 'History', NULL, NULL, 'matched', NULL, 'failed', NULL, 'bibtex_import', 'x', NULL, NULL, '2025-01-01 00:00:07'),
  (8,  'Gamma', NULL, 2005, NULL, NULL, NULL, NULL, NULL, 'pending', NULL, 'not_requested', NULL, NULL, 'refs/metadata.bib', NULL, NULL, '2025-01-01 00:00:08'),
  (9,  'Delta', '["Solo Author"]', 1990, NULL, NULL, NULL, NULL, NULL, char(160) || 'matched' || char(12288), NULL, 'not_requested', NULL, 'pdf', 'pdfB', NULL, NULL, '2025-01-01 00:00:09'),
  (10, 'Epsilon', NULL, 1990, NULL, NULL, NULL, NULL, NULL, 'pending', NULL, 'not_requested', NULL, NULL, 'pdfC', NULL, NULL, '2025-01-01 00:00:10'),
  (11, 'Eta', NULL, 'c. 1980', NULL, NULL, NULL, NULL, NULL, 'pending', NULL, 'not_requested', NULL, NULL, 'unknownkey', NULL, NULL, '2025-01-01 00:00:11'),
  (12, 'Theta', '["' || char(65279) || 'Zed Bom", "Other"]', 1970, NULL, NULL, NULL, NULL, NULL, 'pending', NULL, 'not_requested', NULL, NULL, NULL, '/data/pdfs/Some Paper.v2.pdf', NULL, '2025-01-01 00:00:12'),
  (13, 'Iota', NULL, '١٩٧٠', NULL, NULL, 'Venue I', NULL, NULL, 'pending', 'openalex', 'not_requested', NULL, NULL, NULL, NULL, NULL, '2025-01-01 00:00:13'),
  (14, NULL, NULL, 1960, NULL, NULL, 'Venue K', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2025-01-01 00:00:14'),
  (15, 'Outside one', NULL, 2020, NULL, NULL, NULL, NULL, NULL, 'pending', NULL, 'not_requested', NULL, 'upload', 'unknownkey', NULL, NULL, '2025-01-02'),
  (16, 'Outside two labour', NULL, 2021, NULL, NULL, NULL, NULL, NULL, 'matched', NULL, 'downloaded', NULL, NULL, NULL, NULL, NULL, '2025-01-03'),
  (17, 'Outside three', NULL, 2022, NULL, NULL, NULL, NULL, NULL, 'pending', NULL, 'not_requested', NULL, NULL, NULL, NULL, 8, NULL);

INSERT INTO corpus_works (corpus_id, work_id, added_at) VALUES
  (1, 1, '2026-01-05 10:00:00'), (1, 2, '2026-01-04 10:00:00'), (1, 3, '2026-01-04 10:00:00'),
  (1, 4, NULL), (1, 5, '2026-01-03 09:00:00'), (1, 6, '2026-01-06 08:00:00'),
  (1, 7, '2026-01-01 00:00:00'), (1, 8, '2026-01-02 00:00:00'), (1, 9, '2026-01-07 00:00:00'),
  (1, 10, '2026-01-07 00:00:00'), (1, 11, '2026-01-08 00:00:00'), (1, 12, '2026-01-09 00:00:00'),
  (1, 13, '2026-01-10 00:00:00'), (1, 14, '2026-01-11 00:00:00'),
  (2, 16, '2026-02-01 00:00:00'), (2, 1, '2026-02-01 00:00:00');

INSERT INTO search_runs (id, query) VALUES (7, 'labour economics'), (8, '');

INSERT INTO ingest_source_metadata (corpus_id, ingest_source, title, authors, source_pdf) VALUES
  (1, 'pdfA', 'Seed Doc A', '["Alice Author"]', NULL),
  (1, 'pdfB', NULL, '["X One", "Y Two", "Z Three"]', '/uploads/seedB.pdf'),
  (2, 'pdfC', 'Seed C', NULL, NULL),
  (1, '', 'Ignored blank key', NULL, NULL);

INSERT INTO seed_candidates_in_corpus (corpus_id, work_id, source_type, source_key, marked_at) VALUES
  (1, 1, 'pdf', 'pdfA', '2026-01-02 00:00:00'),
  (1, 1, 'search', '7', '2026-01-01 00:00:00'),
  (1, 2, 'Search', '7', '2026-01-03 00:00:00'),
  (1, 11, 'Manual', ' mk ', '2026-01-04 00:00:00'),
  (1, NULL, 'pdf', 'pdfA', '2026-01-05 00:00:00'),
  (2, 16, 'pdf', 'pdfC', '2026-01-06 00:00:00');
