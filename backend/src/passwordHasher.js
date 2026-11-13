// bcrypt off the event loop: a small worker_threads pool. At most `size`
// hashes run at once and up to `maxQueue` more wait in FIFO order; beyond that
// a call rejects at once with code 'HASHER_BUSY', which the routes answer with
// 503. The bound is also what keeps a login burst from exhausting the CPU.
import { Worker } from 'node:worker_threads';

const WORKER_URL = new URL('./passwordWorker.js', import.meta.url);

function busyError() {
  const error = new Error('Password hasher queue is full');
  error.code = 'HASHER_BUSY';
  return error;
}

// `onWorkerResult({ op, threadId })`, when given, is called for every answer
// with the id of the thread that computed it (tests use it as evidence that
// bcrypt runs off the main thread).
export function createPasswordHasher({ size = 2, maxQueue = 64, unref = false, onWorkerResult = null } = {}) {
  const workers = new Set();
  const idle = [];
  const queue = [];
  let nextId = 1;
  let closed = false;

  function settle(worker, error, result) {
    const job = worker.job;
    if (!job) return;
    worker.job = null;
    if (error) job.reject(error);
    else job.resolve(result);
  }

  function spawn() {
    const worker = new Worker(WORKER_URL);
    worker.job = null;
    worker.on('message', (msg) => {
      if (!worker.job || msg?.id !== worker.job.id) return;
      if (onWorkerResult) {
        try {
          onWorkerResult({ op: worker.job.message.op, threadId: msg.threadId });
        } catch {
          // an observer never breaks the pool
        }
      }
      settle(worker, msg.error ? new Error(msg.error) : null, msg.result);
      release(worker);
    });
    worker.on('error', (error) => {
      settle(worker, new Error(`password worker failed: ${error?.message || error}`));
    });
    worker.on('exit', (code) => {
      workers.delete(worker);
      const at = idle.indexOf(worker);
      if (at >= 0) idle.splice(at, 1);
      settle(worker, new Error(closed ? 'password hasher closed' : `password worker exited with code ${code}`));
      // The replacement is spawned on demand: right away when work is waiting.
      pump();
    });
    workers.add(worker);
    return worker;
  }

  // With `unref`, a worker holds the process open only while it computes: an
  // idle pool keeps neither the server nor Jest alive, and a pending hash is
  // never dropped by an exit.
  function run(worker, job) {
    worker.job = job;
    if (unref) worker.ref();
    worker.postMessage({ id: job.id, ...job.message });
  }

  function release(worker) {
    if (closed || !workers.has(worker)) return;
    const next = queue.shift();
    if (next) {
      run(worker, next);
    } else {
      idle.push(worker);
      if (unref) worker.unref();
    }
  }

  function takeWorker() {
    if (idle.length > 0) return idle.pop();
    if (workers.size < size) return spawn();
    return null;
  }

  function pump() {
    while (!closed && queue.length > 0) {
      const worker = takeWorker();
      if (!worker) return;
      run(worker, queue.shift());
    }
  }

  function submit(message) {
    return new Promise((resolve, reject) => {
      if (closed) {
        reject(new Error('password hasher closed'));
        return;
      }
      const job = { id: nextId++, message, resolve, reject };
      const worker = takeWorker();
      if (worker) run(worker, job);
      else if (queue.length >= maxQueue) reject(busyError());
      else queue.push(job);
    });
  }

  return {
    hash(password, rounds = 10) {
      return submit({ op: 'hash', password: String(password), rounds });
    },
    compare(password, hash) {
      return submit({ op: 'compare', password: String(password), hash: String(hash ?? '') });
    },
    async close() {
      if (closed) return;
      closed = true;
      for (const job of queue.splice(0)) job.reject(new Error('password hasher closed'));
      idle.length = 0;
      await Promise.all([...workers].map((worker) => worker.terminate()));
    },
    get closed() {
      return closed;
    },
    // Live worker threads, for tests and diagnostics.
    workers() {
      return [...workers];
    },
  };
}

let shared = null;

// The process-wide pool. Its idle workers are unref()'d so they keep neither
// the server nor Jest alive.
export function getPasswordHasher() {
  if (!shared || shared.closed) shared = createPasswordHasher({ unref: true });
  return shared;
}
