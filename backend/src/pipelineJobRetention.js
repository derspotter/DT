// Deletes finished pipeline jobs (completed, failed, cancelled) 30 days after
// they finished; pending and running jobs are never touched. Both statements
// are shaped for idx_pipeline_jobs_status_finished (status, finished_at). The
// second is the fallback for legacy jobs that never got a finished_at.
export const PRUNE_STATEMENTS = Object.freeze([
  `DELETE FROM pipeline_jobs
 WHERE status IN ('completed', 'failed', 'cancelled') AND finished_at < datetime('now', ?)`,
  `DELETE FROM pipeline_jobs
 WHERE status IN ('completed', 'failed', 'cancelled') AND finished_at IS NULL
   AND created_at < datetime('now', ?)`,
]);

// A worker holding the write lock blocks the event loop for at most this long.
export const PRUNE_BUSY_TIMEOUT_MS = 250;

const DAY_MS = 24 * 60 * 60 * 1000;

export function pruneFinishedPipelineJobs(db, { days = 30 } = {}) {
  const table = db
    .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pipeline_jobs'")
    .get();
  if (!table) return 0;
  const modifier = `-${days} days`;
  const statements = PRUNE_STATEMENTS.map((sql) => db.prepare(sql));
  const prune = db.transaction(() =>
    statements.reduce((sum, statement) => sum + statement.run(modifier).changes, 0)
  );
  const priorBusyTimeout = db.pragma('busy_timeout', { simple: true });
  db.pragma(`busy_timeout = ${PRUNE_BUSY_TIMEOUT_MS}`);
  try {
    return prune();
  } finally {
    db.pragma(`busy_timeout = ${Number(priorBusyTimeout)}`);
  }
}

export function startPipelineJobRetention(db, { intervalMs = DAY_MS } = {}) {
  const run = () => {
    try {
      const n = pruneFinishedPipelineJobs(db);
      if (n > 0) console.log(`[pipeline-jobs] pruned ${n} finished job(s) older than 30 days`);
    } catch (err) {
      console.error(`[pipeline-jobs] retention failed: ${err?.message ?? err}`);
    }
  };
  run();
  const timer = setInterval(run, intervalMs);
  timer.unref?.();
  return () => clearInterval(timer);
}
