import { escapeHtml, tokenMailContent } from '../src/authMail.js'

test('escapeHtml escapes the five HTML-significant characters', () => {
  expect(escapeHtml(`<a href="x">'&'</a>`)).toBe('&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;')
  expect(escapeHtml(null)).toBe('')
  expect(escapeHtml(undefined)).toBe('')
  expect(escapeHtml(42)).toBe('42')
})

test('invite and reset mails escape the username and the url', () => {
  for (const kind of ['invite', 'reset']) {
    const { html, text } = tokenMailContent({ kind, username: '<b>x</b>&"\'', url: 'https://h/#/a?token=a"b' })
    expect(html).toContain('&lt;b&gt;x&lt;/b&gt;&amp;&quot;&#39;')
    expect(html).not.toContain('<b>x</b>')
    expect(html).toContain('href="https://h/#/a?token=a&quot;b"')
    expect(text).toContain('a"b')
  }
})

test('subjects and expiry lines are unchanged', () => {
  const invite = tokenMailContent({ kind: 'invite', username: 'ann', url: 'https://h/#/accept-invite?token=t' })
  expect(invite.subject).toBe('Set your Korpus Builder password')
  expect(invite.html).toContain('<h2>Set your Korpus Builder password</h2>')
  expect(invite.html).toContain('<p><strong>Username:</strong> ann</p>')
  expect(invite.html).toContain('This link expires in 72 hours.')
  expect(invite.text).toBe(
    'An account has been created for you in Korpus Builder.\n\nUsername: ann\n\nSet your password here:\nhttps://h/#/accept-invite?token=t\n\nThis link expires in 72 hours.'
  )

  const reset = tokenMailContent({ kind: 'reset', username: 'bob', url: 'https://h/#/reset-password?token=t' })
  expect(reset.subject).toBe('Reset your Korpus Builder password')
  expect(reset.html).toContain('<h2>Reset your Korpus Builder password</h2>')
  expect(reset.html).toContain('<p>Hello bob,</p>')
  expect(reset.html).toContain('This link expires in 2 hours.')
  expect(reset.text).toBe('Reset your Korpus Builder password:\nhttps://h/#/reset-password?token=t\n\nThis link expires in 2 hours.')
})

test('an unknown kind is rejected', () => {
  expect(() => tokenMailContent({ kind: 'other', username: 'x', url: 'u' })).toThrow(/kind/)
})
