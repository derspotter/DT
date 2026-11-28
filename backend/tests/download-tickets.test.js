import { createTicketStore } from '../src/downloadTickets.js'

// One ticket store for both download routes (D11): the token stands in for
// the session on one file URL for ttlMs, then it is gone.
test('a ticket is valid for its TTL and pruned after it', () => {
  let t = 0
  const s = createTicketStore({ now: () => t })
  expect(s.ttlSeconds).toBe(60)

  const tok = s.mint({ userId: 1, itemId: 5 })
  expect(tok).toMatch(/^[A-Za-z0-9_-]+$/)
  expect(tok).toHaveLength(32)
  expect(s.get(tok)).toMatchObject({ userId: 1, itemId: 5, expiresAt: 60_000 })
  // Reusable until it expires, as before.
  t = 59_999
  expect(s.get(tok)).toMatchObject({ userId: 1, itemId: 5 })
  t = 60_000
  expect(s.get(tok)).toBeNull()

  // Expired tickets are dropped, not just hidden: with the clock turned back
  // the old ticket stays gone. A mint prunes on its own, without a get.
  t = 0
  expect(s.get(tok)).toBeNull()
  const old = s.mint({ userId: 9 })
  t = 60_000
  const kept = s.mint({ userId: 2, itemId: 6 })
  t = 0
  expect(s.get(old)).toBeNull()
  expect(s.get(kept)).toMatchObject({ userId: 2, itemId: 6 })
  expect(s.get('unknown')).toBeNull()

  const fresh = s.mint({ userId: 3, itemId: 7 })
  expect(fresh).not.toBe(kept)
  expect(s.get(fresh)).toMatchObject({ userId: 3 })
  s.revoke(fresh)
  expect(s.get(fresh)).toBeNull()
  expect(s.get(kept)).toMatchObject({ userId: 2 })
})

test('the TTL is configurable', () => {
  let t = 1_000
  const s = createTicketStore({ ttlMs: 5_000, now: () => t })
  expect(s.ttlSeconds).toBe(5)
  const tok = s.mint({ userId: 1 })
  expect(s.get(tok)).toMatchObject({ userId: 1, expiresAt: 6_000 })
  t = 6_000
  expect(s.get(tok)).toBeNull()
})
