// The ingest API's historical counts include both waiting and active work.
export function seedActivity(stats = {}) {
  const totalMetadata = Number(stats.raw_pending || 0)
  const totalDownloads = Number(stats.queued_download || 0)
  const detailed = stats.enriching != null && stats.downloading != null
  const enriching = Number(stats.enriching || 0)
  const downloading = Number(stats.downloading || 0)
  const parts = detailed
    ? [
        [Math.max(0, totalMetadata - enriching), 'waiting for metadata'],
        [enriching, 'enriching'],
        [Math.max(0, totalDownloads - downloading), 'waiting for download'],
        [downloading, 'downloading'],
      ]
    : [[totalMetadata, 'metadata pending or running'], [totalDownloads, 'downloads pending or running']]
  return {
    text: parts.filter(([count]) => count > 0).map(([count, label]) => `${count} ${label}`).join(' · '),
    active: detailed && enriching + downloading > 0,
  }
}

export function seedPromotionStatus(result, count) {
  const base = result?.message || `Processed ${Number(count).toLocaleString('en-US')} candidate(s). Corpus refreshed.`
  if (result?.expansion_error) {
    // HTTP 200 only confirms promotion; expansion may have failed separately.
    return { warning: true, text: `Corpus promotion completed, but reference expansion failed: ${result.expansion_error}` }
  }
  if (result?.expansion_warning) {
    // An OpenAlex quota stop: the seeds that were built are kept; the server's message
    // already ends with the note, otherwise it is appended here.
    const text = result?.message ? base : `${base} ${result.expansion_warning}`
    return { warning: true, text }
  }
  if (result?.message) return { warning: false, text: base }
  if (result?.promotion_mode === 'new_seed') {
    const seeds = result.expansion_seeds || []
    return { warning: false, text: `${base} ${seeds.length ? `${seeds.length} expansion seed(s) added for review.` : 'No related works were found to seed.'}` }
  }
  return { warning: false, text: base }
}

// The subtitle of a search seed's run. A run that finished ('done') with a note was
// stopped early by OpenAlex quota and kept what it had fetched.
export function searchRunSubtitle(run) {
  if (!run || !run.status) return ''
  if (run.status === 'done') return run.error ? String(run.error) : ''
  const fetched = Number(run.fetched_count || 0).toLocaleString('en-US')
  const expected = run.expected_count === null || run.expected_count === undefined ? '…' : Number(run.expected_count).toLocaleString('en-US')
  if (run.status === 'running') return `fetching ${fetched} of ${expected}`
  if (run.status === 'failed') return `stopped after ${fetched} of ${expected}: ${run.error || 'unknown error'}`
  if (run.status === 'cancelled') return `cancelled at ${fetched} of ${expected}`
  return ''
}
