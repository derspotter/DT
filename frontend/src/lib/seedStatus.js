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
  if (result?.message) return { warning: false, text: base }
  if (result?.promotion_mode === 'new_seed') {
    const seeds = result.expansion_seeds || []
    return { warning: false, text: `${base} ${seeds.length ? `${seeds.length} expansion seed(s) added for review.` : 'No related works were found to seed.'}` }
  }
  return { warning: false, text: base }
}
