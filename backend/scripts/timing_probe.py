#!/usr/bin/env python3
"""Read-only timing probe: old vs new SQL shapes on a live literature.db.

Opens the database with mode=ro and PRAGMA query_only. Prints a markdown
table of median milliseconds (3 runs after one warm-up).
"""
import argparse
import sqlite3
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corpus_list import normalize_year  # noqa: E402  (the oracle's int(): text years -> 0)

DEFAULT_DB = Path(__file__).resolve().parents[2] / "dl_lit_project/data/literature.db"

WS = ("char(9, 10, 11, 12, 13, 28, 29, 30, 31, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196, "
      "8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288)")
META = f"lower(trim(COALESCE(w.metadata_status, ''), {WS}))"
DOWNLOAD = f"lower(trim(COALESCE(w.download_status, ''), {WS}))"
STATUS_SQL = f"""CASE
  WHEN {DOWNLOAD} = 'downloaded' THEN 'downloaded'
  WHEN {DOWNLOAD} IN ('queued', 'in_progress') THEN 'queued_download'
  WHEN {DOWNLOAD} = 'failed' THEN 'failed_download'
  WHEN {META} = 'in_progress' THEN 'enriching'
  WHEN {META} = 'failed' THEN 'failed_enrichment'
  WHEN {META} = 'matched' THEN 'matched'
  ELSE 'raw' END"""
YEAR_SQL = ("CASE WHEN typeof(w.year) = 'integer' THEN w.year WHEN w.year IS NULL THEN 0 "
            "ELSE dt_corpus_int(w.year) END")
ADDED_OLD = "CASE WHEN cw.added_at IS NULL OR cw.added_at = '' THEN '' ELSE CAST(cw.added_at AS TEXT) END"
# Page column subset (the app selects a few more plain columns; same cost class).
COLS = "w.id, w.title, w.authors, w.year, {added} AS added_at, w.source, w.doi, w.openalex_id, w.file_path"
COLS_STATUS = COLS + ", " + STATUS_SQL + " AS status, w.metadata_status, w.download_status"
LIVE_META = "SUM(CASE WHEN w.metadata_status = 'in_progress' AND w.metadata_lease_expires_at > ? THEN 1 ELSE 0 END)"
LIVE_DL = "SUM(CASE WHEN w.download_status = 'in_progress' AND w.download_lease_expires_at > ? THEN 1 ELSE 0 END)"


def connect(db_path):
    uri = f"file:{db_path}?mode=ro"
    last = None
    for _ in range(4):
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=5)
            conn.execute("PRAGMA query_only = ON")
            conn.create_function("dt_corpus_int", 1, normalize_year)
            conn.execute("SELECT count(*) FROM sqlite_master").fetchone()
            return conn
        except sqlite3.OperationalError as exc:
            last = exc
            time.sleep(1)
    raise SystemExit(f"cannot open read-only: {last}")


def median_ms(conn, sql, args=()):
    def run():
        t = time.perf_counter()
        conn.execute(sql, args).fetchall()
        return (time.perf_counter() - t) * 1000
    run()
    return statistics.median(run() for _ in range(3))


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    a = ap.parse_args()
    conn = connect(a.db)

    sizes = conn.execute("SELECT corpus_id, COUNT(*) n FROM corpus_works GROUP BY corpus_id ORDER BY n DESC").fetchall()
    big = sizes[0][0]
    small = next((cid for cid, n in reversed(sizes) if n >= 200), sizes[-1][0])
    sizes_d = dict(sizes)
    oa = conn.execute("SELECT openalex_id FROM works WHERE openalex_id != '' LIMIT 1 OFFSET 50000").fetchone()
    oa = oa[0] if oa else conn.execute("SELECT openalex_id FROM works WHERE openalex_id != '' LIMIT 1").fetchone()[0]
    nowsec = int(time.time())
    rows = []

    def add(name, old, new, note=""):
        o = median_ms(conn, *old)
        n = median_ms(conn, *new)
        rows.append((name, o, n, note))

    add("dedupe by openalex_id",
        ("SELECT id FROM works WHERE openalex_id = ?", (oa,)),
        ("SELECT id FROM works WHERE openalex_id = ? AND openalex_id != ''", (oa,)))

    enrich_old = ("SELECT nm.id FROM works nm WHERE (COALESCE(metadata_status,'pending') = 'pending' "
                  "OR (metadata_status = 'in_progress' AND metadata_lease_expires_at IS NOT NULL AND metadata_lease_expires_at <= ?)) "
                  "AND COALESCE(nm.download_status,'not_requested') = 'not_requested' {x} ORDER BY nm.id ASC LIMIT 10")
    enrich_new = ("SELECT nm.id FROM works nm WHERE (metadata_status = 'pending' "
                  "OR (metadata_status = 'in_progress' AND metadata_lease_expires_at IS NOT NULL AND metadata_lease_expires_at <= ?)) "
                  "AND nm.download_status = 'not_requested' {x} ORDER BY nm.id ASC LIMIT 10")
    ex = "AND EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.work_id = nm.id AND cw.corpus_id = ?)"
    add("enrich claim select (all)", (enrich_old.format(x=""), (nowsec,)), (enrich_new.format(x=""), (nowsec,)))
    add(f"enrich claim select (corpus {big})", (enrich_old.format(x=ex), (nowsec, big)), (enrich_new.format(x=ex), (nowsec, big)))

    # Mirrors pipelineReads.SMALL_CORPUS_LIMIT / corpusIsSmall.
    small_limit = 5000
    size_probe = "SELECT COUNT(*) FROM (SELECT 1 FROM corpus_works WHERE corpus_id = ? LIMIT ?)"

    def is_small(cid):
        return conn.execute(size_probe, (cid, small_limit + 1)).fetchone()[0] <= small_limit

    grouped = (f"SELECT w.metadata_status, w.download_status, COUNT(*) AS n, {LIVE_META} AS live_meta, {LIVE_DL} AS live_dl ")
    exists_from = "FROM works w WHERE EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.corpus_id = ? AND cw.work_id = w.id) "
    join_from = "FROM corpus_works cw JOIN works w ON w.id = cw.work_id WHERE cw.corpus_id = ? "

    def counts(cid):
        old = (f"SELECT {STATUS_SQL} AS status, COUNT(*) AS n FROM works w JOIN corpus_works cw ON cw.work_id = w.id "
               "WHERE cw.corpus_id = ? GROUP BY 1", (cid,))
        new = (grouped + (join_from if is_small(cid) else exists_from) + "GROUP BY 1, 2", (nowsec, nowsec, cid))
        return old, new

    def exists_note(cid):
        if not is_small(cid):
            return "EXISTS form (large corpus)"
        t8 = median_ms(conn, grouped + exists_from + "GROUP BY 1, 2", (nowsec, nowsec, cid))
        return f"JOIN form (small corpus); EXISTS form would take {t8:.2f}"
    add(f"status counts, largest (corpus {big}, {sizes_d[big]} works)", *counts(big), exists_note(big))
    small_ids = [small] + [cid for cid in (102, 130) if cid in sizes_d and cid != small]
    for cid in small_ids:
        add(f"status counts, small (corpus {cid}, {sizes_d[cid]} works)", *counts(cid), exists_note(cid))
    rows.append(("corpus size-class probe", None, median_ms(conn, size_probe, (big, small_limit + 1)), f"corpus {big}"))

    probe = ("SELECT EXISTS (SELECT 1 FROM works WHERE year <= 0) "
             "OR EXISTS (SELECT 1 FROM works WHERE typeof(year) NOT IN ('integer', 'null'))")
    only_int = not conn.execute(probe).fetchone()[0]
    rows.append(("year-order probe (memoised)", None, median_ms(conn, probe),
                 "years positive integer/NULL only" if only_int else "CASE PATH: non-integer or <= 0 years present"))

    cols_old = COLS_STATUS.format(added="cw.added_at")
    member = "FROM corpus_works cw WHERE cw.corpus_id = ? AND cw.work_id = w.id"
    cols_new = COLS_STATUS.format(added=f"(SELECT cw.added_at {member})")
    page_old = (f"SELECT {cols_old} FROM works w JOIN corpus_works cw ON cw.work_id = w.id WHERE cw.corpus_id = ? "
                f"ORDER BY {YEAR_SQL} DESC, w.id DESC LIMIT 200 OFFSET 0")
    page_idx = (f"SELECT {cols_new} FROM works w WHERE EXISTS (SELECT 1 {member}) "
                "ORDER BY w.year DESC, w.id DESC LIMIT 200 OFFSET 0")

    def page_new(cid):
        if only_int and not is_small(cid):
            return page_idx, (cid, cid)
        return page_old, (cid,)

    def page_note(cid):
        if not only_int:
            return "CASE path (years)"
        if not is_small(cid):
            return "index path (large corpus)"
        return f"JOIN + YEAR_SQL (small corpus); index path would take {median_ms(conn, page_idx, (cid, cid)):.2f}"
    for cid in [big] + small_ids:
        add(f"default page, corpus {cid} ({sizes_d[cid]} works)", (page_old, (cid,)), page_new(cid), page_note(cid))
    cols_add = COLS_STATUS.format(added="cw.added_at")
    add(f"added-sort page, corpus {big}",
        (f"SELECT {cols_add} FROM works w JOIN corpus_works cw ON cw.work_id = w.id WHERE cw.corpus_id = ? "
         f"ORDER BY {ADDED_OLD} DESC, w.id DESC LIMIT 200 OFFSET 0", (big,)),
        (f"SELECT {cols_add} FROM corpus_works cw JOIN works w ON w.id = cw.work_id WHERE cw.corpus_id = ? "
         "ORDER BY cw.added_at DESC, cw.work_id DESC LIMIT 200 OFFSET 0", (big,)))

    print(f"works rows: {conn.execute('SELECT COUNT(*) FROM works').fetchone()[0]}; corpora (id:n): "
          + ", ".join(f"{c}:{n}" for c, n in sizes[:5]) + "\n")
    print("| measurement | old ms | new ms | note |\n|---|---:|---:|---|")
    for name, o, n, note in rows:
        print(f"| {name} | {'-' if o is None else f'{o:.2f}'} | {n:.2f} | {note} |")


if __name__ == "__main__":
    sys.exit(main())
