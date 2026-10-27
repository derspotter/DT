#!/usr/bin/env python3
"""Read-only timing probe: old vs new SQL shapes on a live literature.db.

Opens the database with mode=ro only and refuses any other URI. Prints a
markdown table of median milliseconds (3 runs after one warm-up).
"""
import argparse
import sqlite3
import statistics
import sys
import time

DEFAULT_DB = "/home/jay/DT/dl_lit_project/data/literature.db"

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
    if "mode=ro" not in uri:
        raise SystemExit("refusing to open without mode=ro")
    last = None
    for _ in range(4):
        try:
            conn = sqlite3.connect(uri, uri=True, timeout=5)
            conn.execute("PRAGMA query_only = ON")
            conn.create_function("dt_corpus_int", 1, lambda v: int(float(v)) if v not in (None, "") else 0)
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

    def counts(cid):
        old = (f"SELECT {STATUS_SQL} AS status, COUNT(*) AS n FROM works w JOIN corpus_works cw ON cw.work_id = w.id "
               "WHERE cw.corpus_id = ? GROUP BY 1", (cid,))
        new = (f"SELECT w.metadata_status, w.download_status, COUNT(*) AS n, {LIVE_META} AS live_meta, {LIVE_DL} AS live_dl "
               "FROM works w WHERE EXISTS (SELECT 1 FROM corpus_works cw WHERE cw.corpus_id = ? AND cw.work_id = w.id) "
               "GROUP BY 1, 2", (nowsec, nowsec, cid))
        return old, new
    add(f"status counts, largest (corpus {big}, {sizes_d[big]} works)", *counts(big))
    add(f"status counts, small (corpus {small}, {sizes_d[small]} works)", *counts(small))

    only_int = conn.execute("SELECT 1 FROM works WHERE typeof(year) NOT IN ('integer','null') LIMIT 1").fetchone() is None
    probe = "SELECT 1 FROM works WHERE typeof(year) NOT IN ('integer', 'null') LIMIT 1"
    rows.append(("year-type probe", None, median_ms(conn, probe), "new only; years integer/NULL only" if only_int else "NON-INTEGER YEARS PRESENT"))

    cols_old = COLS_STATUS.format(added="cw.added_at")
    member = "FROM corpus_works cw WHERE cw.corpus_id = ? AND cw.work_id = w.id"
    cols_new = COLS_STATUS.format(added=f"(SELECT cw.added_at {member})")
    if only_int:
        add(f"default page, corpus {big}",
            (f"SELECT {cols_old} FROM works w JOIN corpus_works cw ON cw.work_id = w.id WHERE cw.corpus_id = ? "
             f"ORDER BY {YEAR_SQL} DESC, w.id DESC LIMIT 200 OFFSET 0", (big,)),
            (f"SELECT {cols_new} FROM works w WHERE EXISTS (SELECT 1 {member}) "
             "ORDER BY w.year DESC, w.id DESC LIMIT 200 OFFSET 0", (big, big)))
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
