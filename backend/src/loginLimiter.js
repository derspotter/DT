// Per-username sign-in limit (BE-40). One limiter lives per app, in memory:
// the backend is a single Node process, so one map sees every sign-in, and a
// restart resets the counts.
//
// A slot is reserved before the password check, so concurrent guesses cannot
// exceed `maxFailures` in one window. Each reservation settles exactly once:
// fail (a guess that missed), refund (no guess was made, e.g. a hasher error)
// or succeed (clears the settled failures). Settlement is bound to the
// reservation's window: after a rollover or an eviction it does nothing.
//
// Two maps: an account that matched a user row (`known`, keyed by the caller
// as `id:<user id>`, so two rows never share a counter) lives in an uncapped
// map, whose size is bounded by the user count, so an existing account is
// never evicted or left untracked. Names with no row live in a map capped at
// `maxKeys`; at capacity it deletes its oldest entry (expired if any is),
// locked or not. Flooding junk names can therefore only evict the lock
// of another unknown name.
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
  const knownEntries = new Map();
  const unknownEntries = new Map();
  // One counter for both maps: two windows never share an id.
  let nextWindowId = 1;
  // Expired entries are swept from both maps at most once per window, so an
  // entry whose key is never seen again (e.g. a deleted invitation) does not
  // stay until restart. A full scan per window keeps the cost amortized O(1).
  let lastSweepAt = null;

  const mapFor = (known) => (known ? knownEntries : unknownEntries);
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

  // Unknown map at capacity: delete its first key. A renewed window moves its
  // entry to the end, so the first key has the oldest window start, expired
  // if any entry is. O(1): no scan per insertion; the periodic sweep below
  // removes the rest of the expired entries.
  const makeRoomForUnknown = () => {
    const oldest = unknownEntries.keys().next();
    if (!oldest.done) unknownEntries.delete(oldest.value);
  };

  const sweepExpired = (at) => {
    if (lastSweepAt !== null && at - lastSweepAt < windowMs) return;
    lastSweepAt = at;
    for (const entries of [knownEntries, unknownEntries]) {
      for (const [key, entry] of entries) {
        if (expired(entry, at)) entries.delete(key);
      }
    }
  };

  // `known`: the key names a user row (active or invited).
  const reserve = (name, { known = false } = {}) => {
    const key = loginLimiterKey(name);
    const entries = mapFor(known);
    const at = now();
    sweepExpired(at);
    let entry = entries.get(key);
    if (!entry) {
      if (!known && unknownEntries.size >= maxKeys) makeRoomForUnknown();
      entry = newWindow({}, at);
      entries.set(key, entry);
    } else if (expired(entry, at)) {
      // re-insert at the end: insertion order stays window-start order
      entries.delete(key);
      entries.set(key, newWindow(entry, at));
    }
    if (locked(entry)) {
      return {
        allowed: false,
        retryAfterSeconds: Math.max(1, Math.ceil((entry.windowStart + windowMs - at) / 1000)),
      };
    }
    entry.inFlight += 1;
    return { allowed: true, ticket: { key, windowId: entry.windowId, known: Boolean(known) } };
  };

  const settle = (ticket, apply) => {
    if (!ticket) return;
    const entries = mapFor(ticket.known);
    const entry = entries.get(ticket.key);
    if (!entry || entry.windowId !== ticket.windowId) return;
    entry.inFlight = Math.max(0, entry.inFlight - 1);
    apply(entry);
    if (entry.failures + entry.inFlight === 0) entries.delete(ticket.key);
  };

  return {
    reserve,
    // Tracked entry counts, for diagnostics and tests.
    size() {
      return { known: knownEntries.size, unknown: unknownEntries.size };
    },
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
