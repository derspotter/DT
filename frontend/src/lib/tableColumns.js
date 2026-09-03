// Shared column set for the Seed (section 2) and Corpus (section 3) tables.
//
// Spec C1 asks both tables to draw from ONE column definition list with a
// per-table visibility toggle; C2 adds the Seed provenance column; line 16
// splits the collapsed status into Metadata and Download; line 19 fixes
// "Source" to mean the venue in both tables.
//
// Definitions are shared, but the on/off state is stored per table so the two
// can be configured independently (spec line 147).

export const COLUMN_DEFS = [
  { key: 'metadata', label: 'Metadata', width: '150px', sortable: true, defaultVisible: true },
  { key: 'download', label: 'Download', width: '140px', sortable: true, defaultVisible: true },
  { key: 'title', label: 'Title', width: 'minmax(220px, 3fr)', sortable: true, defaultVisible: true },
  { key: 'authors', label: 'Authors', width: 'minmax(140px, 1.5fr)', sortable: true, defaultVisible: true },
  { key: 'year', label: 'Year', width: '70px', sortable: true, defaultVisible: true },
  // Round 2 item 7: how many works the item cites on OpenAlex — what a
  // downstream promote pulls in. Seed table only this round.
  { key: 'refs', label: 'Refs', width: '70px', sortable: true, defaultVisible: true, tables: ['seed'],
    hint: 'Works this item cites on OpenAlex — what a downstream promote pulls in' },
  { key: 'cited_by', label: 'Cited by', width: '80px', sortable: true, defaultVisible: false, tables: ['seed'],
    hint: 'How often OpenAlex has seen this item cited' },
  // "Source" is the article's venue in BOTH tables — never provenance.
  { key: 'source', label: 'Source', width: 'minmax(120px, 1.5fr)', sortable: true, defaultVisible: true },
  // "Seed" is the provenance: the search query or the seed document. In the
  // seed table it is implicit (it is the seed you are inside), so it is off there.
  { key: 'seed', label: 'Seed', width: 'minmax(120px, 1.5fr)', sortable: true, defaultVisible: true, tables: ['corpus'] },
  // Seed-only action column holding the inline promote / in-corpus check.
  { key: 'corpus', label: 'Corpus', width: '110px', sortable: false, defaultVisible: true, tables: ['seed'] },
  { key: 'doi', label: 'DOI', width: 'minmax(120px, 1.2fr)', sortable: false, defaultVisible: false },
  { key: 'publisher', label: 'Publisher', width: 'minmax(120px, 1.2fr)', sortable: false, defaultVisible: false },
  { key: 'type', label: 'Type', width: '110px', sortable: false, defaultVisible: false },
  { key: 'openalex', label: 'OpenAlex', width: '120px', sortable: false, defaultVisible: false },
  { key: 'pages', label: 'Pages', width: '90px', sortable: false, defaultVisible: false },
  { key: 'open_access', label: 'Open access', width: '110px', sortable: false, defaultVisible: false },
  { key: 'file', label: 'File', width: '80px', sortable: false, defaultVisible: false },
]

export function columnsForTable(table) {
  return COLUMN_DEFS.filter((def) => !def.tables || def.tables.includes(table))
}

function storageKey(table) {
  return `rag_feeder_columns_${table}`
}

export function defaultVisibility(table) {
  const out = {}
  for (const def of columnsForTable(table)) out[def.key] = Boolean(def.defaultVisible)
  return out
}

// Persisted per browser (spec line 146) — no backend involved.
export function loadVisibility(table) {
  const defaults = defaultVisibility(table)
  try {
    const raw = localStorage.getItem(storageKey(table))
    if (!raw) return defaults
    const parsed = JSON.parse(raw)
    if (!parsed || typeof parsed !== 'object') return defaults
    // Merge onto defaults so a column added in a later release shows up
    // instead of being silently hidden by a stale stored object.
    return { ...defaults, ...parsed }
  } catch {
    return defaults
  }
}

export function saveVisibility(table, visibility) {
  try {
    localStorage.setItem(storageKey(table), JSON.stringify(visibility))
  } catch {
    // A full or unavailable localStorage must not break the table.
  }
}

export function visibleColumns(table, visibility) {
  return columnsForTable(table).filter((def) => Boolean(visibility?.[def.key]))
}

export function gridTemplate(columns) {
  return columns.map((def) => def.width).join(' ')
}
