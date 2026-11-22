// Per-username sign-in limit (BE-40). One limiter lives per app, in memory:
// the backend is a single Node process, so one map sees every sign-in, and a
// restart resets the counts.
//
// A slot is reserved before the password check, so concurrent guesses cannot
// exceed `maxFailures` in one window. Each reservation settles exactly once:
// fail (a guess that missed), refund (no guess was made, e.g. a hasher or
// lookup error) or succeed (clears the settled failures). Settlement is bound
// to the reservation's window: after a rollover or an eviction it does nothing.
import crypto from 'crypto';

// Map keys are fixed-size digests, so a request-sized username costs no more
// memory than a short one; equality stays exact.
export function loginLimiterKey(name) {
  return crypto.createHash('sha256').update(name).digest('hex');
}

export function createLoginLimiter({
  maxFailures = 10,
  windowMs = 15 * 60 * 1000,
  maxKeys = 10_000,
  now = () => Date.now(),
} = {}) {
  // key -> { windowId, windowStart, failures, inFlight }, in insertion order
  const entries = new Map();
  let nextWindowId = 1;

  const expired = (entry, at) => at - entry.windowStart >= windowMs;
  const locked = (entry) => entry.failures + entry.inFlight >= maxFailures;
  const newWindow = (entry, at) => {
    entry.windowId = nextWindowId;
    nextWindowId += 1;
    entry.windowStart = at;
    entry.failures = 0;
    entry.inFlight = 0;
    return entry;
  };

  // Called only at capacity: delete every expired entry; if none was, delete
  // the oldest entry that is not locked. Returns whether room was made.
  const makeRoom = (at) => {
    let removed = false;
    for (const [key, entry] of entries) {
      if (expired(entry, at)) {
        entries.delete(key);
        removed = true;
      }
    }
    if (removed) return true;
    for (const [key, entry] of entries) {
      if (!locked(entry)) {
        entries.delete(key);
        return true;
      }
    }
    return false;
  };

  const reserve = (name) => {
    const key = loginLimiterKey(name);
    const at = now();
    let entry = entries.get(key);
    if (!entry) {
      if (entries.size >= maxKeys && !makeRoom(at)) {
        // Every tracked entry is locked: fail open for the new key.
        return { allowed: true, ticket: null };
      }
      entry = newWindow({}, at);
      entries.set(key, entry);
    } else if (expired(entry, at)) {
      newWindow(entry, at);
    }
    if (locked(entry)) {
      return {
        allowed: false,
        retryAfterSeconds: Math.max(1, Math.ceil((entry.windowStart + windowMs - at) / 1000)),
      };
    }
    entry.inFlight += 1;
    return { allowed: true, ticket: { key, windowId: entry.windowId } };
  };

  const settle = (ticket, apply) => {
    if (!ticket) return;
    const entry = entries.get(ticket.key);
    if (!entry || entry.windowId !== ticket.windowId) return;
    entry.inFlight = Math.max(0, entry.inFlight - 1);
    apply(entry);
    if (entry.failures + entry.inFlight === 0) entries.delete(ticket.key);
  };

  return {
    reserve,
    fail(ticket) {
      settle(ticket, (entry) => {
        entry.failures += 1;
      });
    },
    refund(ticket) {
      settle(ticket, () => {});
    },
    succeed(ticket) {
      settle(ticket, (entry) => {
        entry.failures = 0;
      });
    },
  };
}
