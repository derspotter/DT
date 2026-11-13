// Invite and password-reset mails. Every interpolated value is escaped in the
// HTML part; the plain-text part carries the values as they are.

const HTML_ESCAPES = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
};

export function escapeHtml(value) {
  if (value === null || value === undefined) return '';
  return String(value).replace(/[&<>"']/g, (ch) => HTML_ESCAPES[ch]);
}

const TOKEN_MAILS = {
  invite: {
    subject: 'Set your Korpus Builder password',
    intro: (u) => `<p>An account has been created for you.</p>
        <p><strong>Username:</strong> ${u}</p>
        <p>Use the link below to set your password and activate the account.</p>`,
    button: 'Set password',
    expiry: 'This link expires in 72 hours.',
    text: ({ username, url }) =>
      `An account has been created for you in Korpus Builder.\n\nUsername: ${username}\n\nSet your password here:\n${url}\n\nThis link expires in 72 hours.`,
  },
  reset: {
    subject: 'Reset your Korpus Builder password',
    intro: (u) => `<p>Hello ${u},</p>
        <p>Use the link below to set a new password.</p>`,
    button: 'Reset password',
    expiry: 'This link expires in 2 hours.',
    text: ({ url }) => `Reset your Korpus Builder password:\n${url}\n\nThis link expires in 2 hours.`,
  },
};

export function tokenMailContent({ kind, username, url }) {
  const mail = TOKEN_MAILS[kind];
  if (!mail) throw new Error(`Unknown token mail kind: ${kind}`);
  const safeUrl = escapeHtml(url);
  const html = `
      <div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#22313d;line-height:1.5">
        <h2>${mail.subject}</h2>
        ${mail.intro(escapeHtml(username))}
        <p><a href="${safeUrl}" style="display:inline-block;padding:12px 18px;border-radius:999px;background:#0f8b8d;color:#fff;text-decoration:none;font-weight:600">${mail.button}</a></p>
        <p>If the button does not work, open this link:</p>
        <p style="word-break:break-all">${safeUrl}</p>
        <p>${mail.expiry}</p>
      </div>
    `;
  return { subject: mail.subject, html, text: mail.text({ username, url }) };
}
