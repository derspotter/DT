// Worker thread for passwordHasher.js: runs bcryptjs synchronously off the
// main thread, so the hash format ($2b$) is the same as before.
import { parentPort } from 'node:worker_threads';
import bcrypt from 'bcryptjs';

parentPort.on('message', ({ id, op, password, hash, rounds }) => {
  try {
    let result;
    if (op === 'hash') {
      result = bcrypt.hashSync(password, rounds);
    } else if (op === 'compare') {
      result = bcrypt.compareSync(password, hash);
    } else {
      throw new Error(`Unknown password worker op: ${op}`);
    }
    parentPort.postMessage({ id, result });
  } catch (error) {
    parentPort.postMessage({ id, error: error?.message || String(error) });
  }
});
