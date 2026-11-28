// Schema probes and JSON parsing shared by the backend read paths. Nothing is
// cached: scripts add columns and tables at run time.

export function tableExists(db, name) {
  return Boolean(db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name = ?").get(name))
}

export function listTableColumns(db, table) {
  if (!tableExists(db, table)) return []
  return db.prepare(`PRAGMA table_info(${table})`).all().map((row) => String(row.name || ''))
}

export function tableHasColumn(db, table, column) {
  return db.prepare(`PRAGMA table_info(${table})`).all().some((row) => row.name === column)
}

// '' / null / undefined give the fallback; an object passes through; a parse
// failure gives the fallback. A JSON 'null' parses to null.
export function parseJson(value, fallback = null) {
  if (value === undefined || value === null || value === '') return fallback
  if (typeof value === 'object') return value
  try {
    return JSON.parse(value)
  } catch {
    return fallback
  }
}
