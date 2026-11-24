import crypto from 'crypto';

// Short-lived download tickets: a token stands in for the session on one file
// URL (an <a download> cannot send the auth header). A ticket stays reusable
// until it expires; mint and get drop expired tickets first.
export function createTicketStore({ ttlMs = 60_000, now = Date.now } = {}) {
  const tickets = new Map(); // token -> { ...fields, expiresAt }
  const prune = () => {
    const current = now();
    for (const [token, ticket] of tickets.entries()) {
      if (!ticket || Number(ticket.expiresAt || 0) <= current) tickets.delete(token);
    }
  };
  return {
    ttlSeconds: Math.round(ttlMs / 1000),
    mint(fields) {
      prune();
      const token = crypto.randomBytes(24).toString('base64url');
      tickets.set(token, { ...fields, expiresAt: now() + ttlMs });
      return token;
    },
    get(token) {
      prune();
      return tickets.get(token) || null;
    },
    revoke(token) {
      tickets.delete(token);
    },
  };
}
