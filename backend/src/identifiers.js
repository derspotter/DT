// One rule per identifier, shared by seed matching and the graph.

// The first W id, uppercased, else null: the Python works.openalex_id rule
// (dl_lit_project identifiers.py), so a URL keys as its bare id.
export function normalizeOpenAlexId(value) {
  const text = String(value || '').trim()
  if (!text) return null
  const match = text.match(/(W\d+)/i)
  return match ? match[1].toUpperCase() : null
}

export function normalizeDoi(doi) {
  if (!doi) return null
  const match = String(doi).match(/(10\.\d{4,9}\/[\-._;()/:A-Z0-9]+)/i)
  if (match) return match[1].toUpperCase()
  return String(doi).trim().toUpperCase() || null
}
