import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { expect, test, type Page, type Route, type WebSocketRoute } from '@playwright/test'
import { fulfil, mockLogSocket } from './helpers/mockApi'

// One sign-out path: a 401 from any request runs the full logout (socket
// closed, token cleared, workspace state reset), and a /me answer requested
// under an earlier session never lands in the next one.

const corpus = { id: 1, name: 'Shared corpus', role: 'owner', owner_username: 'alice' }
const userA = { id: 1, username: 'alice', last_corpus_id: 1, is_admin: false }
const userB = { id: 2, username: 'bob', last_corpus_id: 1, is_admin: false }

const seedSource = {
  id: 'keyword:one',
  source_type: 'keyword',
  source_key: 'one',
  label: 'Seed One',
  candidate_count: 2,
}
const candidates = [
  { candidate_key: 'c1', title: 'Candidate Alpha', year: 2020 },
  { candidate_key: 'c2', title: 'Candidate Beta', year: 2021 },
]

// Permissive defaults so the workspace renders; `hook` sees every request
// first and returns true when it answered it.
function workspaceDefaults(path: string, route: Route) {
  if (path === '/api/corpus') return fulfil(route, { items: [], total: 0, source: 'api', stage_totals: {} })
  if (path === '/api/recursion-config') return fulfil(route, { keyword: {} })
  if (path === '/api/openalex/quota') return fulfil(route, { available: false })
  if (path === '/api/seed/sources') return fulfil(route, { sources: [seedSource], total: 1, source: 'api' })
  if (path.startsWith('/api/seed/sources/keyword/one/candidates')) {
    return fulfil(route, { candidates, total: 2, limit: 200 })
  }
  if (path === '/api/ingest/runs') return fulfil(route, { runs: [], total: 0, source: 'api' })
  if (path === '/api/ingest/stats') return fulfil(route, { stats: {} })
  return fulfil(route, {})
}

async function mockSocket(page: Page, sockets: { opened: number; closed: number }) {
  await mockLogSocket(page, (ws: WebSocketRoute) => {
    sockets.opened += 1
    ws.onClose(() => { sockets.closed += 1 })
  })
}

async function logIn(page: Page, username: string) {
  const form = page.getByTestId('login-form')
  await expect(form).toBeVisible()
  await form.getByLabel('Username').fill(username)
  await form.getByLabel('Password').fill('secret-password')
  await form.getByRole('button', { name: 'Sign in' }).click()
}

test('a 401 on a scoped request signs out completely', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  let expired = false
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-first')
    }
  })
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/login') {
      expired = false
      return fulfil(route, { token: 'token-second', user: userA })
    }
    if (expired) return fulfil(route, { error: 'Unauthorized' }, 401)
    if (path === '/api/auth/me') return fulfil(route, { user: userA, corpora: [corpus] })
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.getByText('Seed One').click()
  await expect(page.getByText('Candidate Alpha')).toBeVisible()
  await expect(page.getByText('Candidate Beta')).toBeVisible()
  await expect.poll(() => sockets.opened).toBe(1)

  expired = true
  await page.evaluate(() => window.dispatchEvent(new Event('focus')))
  await expect(page.getByTestId('login-form')).toBeVisible()
  await expect.poll(() => sockets.closed).toBe(1)
  expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBeNull()

  await logIn(page, 'alice')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await expect(page.getByText('Seed One')).toBeVisible()
  await expect(page.getByText('Candidate Alpha')).toHaveCount(0)
  await expect(page.getByText('Candidate Beta')).toHaveCount(0)
  expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBe('token-second')
})

test('expired token at boot renders the login form after one unauthorized handling', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  const pageErrors: string[] = []
  // The Vite dev client's HMR socket can drop under the test runner; that is
  // the dev server, not the app.
  page.on('pageerror', (error) => {
    if (!/WebSocket closed without opened/.test(error.message)) pageErrors.push(error.message)
  })
  let meRequests = 0
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'expired-token')
    // Every run of the sign-out path clears the token exactly once.
    const removeItem = Storage.prototype.removeItem
    const counter = window as unknown as { __tokenClears: number }
    counter.__tokenClears = 0
    Storage.prototype.removeItem = function (key: string) {
      if (key === 'rag_feeder_token') counter.__tokenClears += 1
      return removeItem.call(this, key)
    }
  })
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') meRequests += 1
    return fulfil(route, { error: 'Unauthorized' }, 401)
  })

  await page.goto('/#/workspace')
  await expect(page.getByTestId('login-form')).toBeVisible()
  await page.waitForTimeout(1000)
  await expect(page.getByTestId('login-form')).toBeVisible()
  expect(meRequests).toBe(1)
  expect(sockets.opened).toBe(0)
  expect(pageErrors).toEqual([])
  // /me and the parallel workspace loads all answered 401: one handling.
  expect(await page.evaluate(() => (window as unknown as { __tokenClears: number }).__tokenClears)).toBe(1)
  expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBeNull()
})

test('a /me answer from before a re-login is discarded', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  let holdNextMe = false
  let heldMeSeen = false
  let heldMeAnswered = false
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: userB })
    if (path === '/api/auth/me') {
      const user = auth.endsWith('token-b') ? userB : userA
      if (holdNextMe && user === userA) {
        holdNextMe = false
        heldMeSeen = true
        await new Promise((resolve) => setTimeout(resolve, 1500))
        await fulfil(route, { user: userA, corpora: [corpus] })
        heldMeAnswered = true
        return
      }
      return fulfil(route, { user, corpora: [corpus] })
    }
    // A live worker lease keeps the poll (and its /me reconcile) at 2 s.
    if (path === '/api/ingest/stats') return fulfil(route, { stats: { enriching_live: 1 } })
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await expect(page.getByTestId('auth-user')).toHaveText('alice')

  holdNextMe = true
  await expect.poll(() => heldMeSeen, { timeout: 10_000 }).toBe(true)
  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  // Record every name the header shows from here on: the next poll would
  // correct a stale overwrite, so the final text alone proves nothing.
  await page.evaluate(() => {
    const seen: string[] = []
    ;(window as unknown as { __authUserSeen: string[] }).__authUserSeen = seen
    new MutationObserver(() => {
      const text = document.querySelector('[data-testid="auth-user"]')?.textContent || ''
      if (seen[seen.length - 1] !== text) seen.push(text)
    }).observe(document.body, { subtree: true, childList: true, characterData: true })
  })

  await expect.poll(() => heldMeAnswered, { timeout: 5000 }).toBe(true)
  await page.waitForTimeout(500)
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  const seen = await page.evaluate(() => (window as unknown as { __authUserSeen: string[] }).__authUserSeen)
  expect(seen).not.toContain('alice')
})

test('corpus action errors show in the corpus action status, not the auth error', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'playwright-token')
  })
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    if (path === '/api/auth/me') return fulfil(route, { user: userA, corpora: [corpus] })
    if (path === '/api/corpora' && request.method() === 'POST') return fulfil(route, { error: 'Name taken' }, 400)
    return workspaceDefaults(path, route)
  })
  page.on('dialog', (dialog) => dialog.accept('Shared corpus'))

  await page.goto('/#/upstream')
  await page.getByRole('button', { name: 'New corpus' }).click()
  await expect(page.locator('.upstream-management-form__status')).toHaveText('Name taken')
  await expect(page.getByTestId('login-form')).toHaveCount(0)
})

test('a failed sign-in attempt that lost the session to a newer one changes nothing', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  let releaseHeldMe: () => void = () => {}
  const heldMeReleased = new Promise<void>((resolve) => { releaseHeldMe = resolve })
  let heldMeSeen = false
  let heldMeAnswered = false
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') {
      const { username } = JSON.parse(request.postData() || '{}')
      return username === 'alice'
        ? fulfil(route, { token: 'token-a', user: userA })
        : fulfil(route, { token: 'token-b', user: userB })
    }
    if (path === '/api/auth/me') {
      if (auth.endsWith('token-a')) {
        // Attempt A's /me is slow and finally answers 401 for A's token.
        heldMeSeen = true
        await heldMeReleased
        await fulfil(route, { error: 'Unauthorized' }, 401)
        heldMeAnswered = true
        return
      }
      if (auth.endsWith('token-b')) return fulfil(route, { user: userB, corpora: [corpus] })
      return fulfil(route, { error: 'Unauthorized' }, 401)
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await logIn(page, 'alice')
  await expect.poll(() => heldMeSeen).toBe(true)
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')

  releaseHeldMe()
  await expect.poll(() => heldMeAnswered).toBe(true)
  await page.waitForTimeout(300)
  await expect(page.getByTestId('login-form')).toHaveCount(0)
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBe('token-b')
})

test('a sign-in attempt that succeeds after a newer one won does not replace its token', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  let releaseLoginA: () => void = () => {}
  const loginAReleased = new Promise<void>((resolve) => { releaseLoginA = resolve })
  let loginASeen = false
  let loginAAnswered = false
  const authHeaders: string[] = []
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') {
      const { username } = JSON.parse(request.postData() || '{}')
      if (username === 'alice') {
        // Attempt A's auth call answers late, after B completed.
        loginASeen = true
        await loginAReleased
        await fulfil(route, { token: 'token-a', user: userA })
        loginAAnswered = true
        return
      }
      return fulfil(route, { token: 'token-b', user: userB })
    }
    authHeaders.push(auth)
    if (path === '/api/auth/me') {
      if (auth.endsWith('token-b')) return fulfil(route, { user: userB, corpora: [corpus] })
      if (auth.endsWith('token-a')) return fulfil(route, { user: userA, corpora: [corpus] })
      return fulfil(route, { error: 'Unauthorized' }, 401)
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await logIn(page, 'alice')
  await expect.poll(() => loginASeen).toBe(true)
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')

  releaseLoginA()
  await expect.poll(() => loginAAnswered).toBe(true)
  await page.waitForTimeout(300)
  expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBe('token-b')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')

  // The next request after A's late answer still carries B's token.
  const seenBefore = authHeaders.length
  await page.waitForTimeout(600)
  await page.evaluate(() => window.dispatchEvent(new Event('focus')))
  await expect.poll(() => authHeaders.length).toBeGreaterThan(seenBefore)
  expect(authHeaders[seenBefore]).toBe('Bearer token-b')
})

test('signing out clears the search form and the admin settings draft', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  const adminA = { ...userA, is_admin: true }
  const adminB = { ...userB, is_admin: true }
  let settingsRequests = 0
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: adminB })
    if (path === '/api/auth/me') {
      return fulfil(route, { user: auth.endsWith('token-b') ? adminB : adminA, corpora: [corpus] })
    }
    if (path === '/api/admin/settings') {
      settingsRequests += 1
      return fulfil(route, { settings: { openalex_api_key: { is_set: false } } })
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await expect(page.getByTestId('auth-user')).toHaveText('alice')
  const author = page.locator('.seed-search-author input')
  const yearFromInput = page.locator('input[type="number"][placeholder="Year"]').first()
  await author.fill('Elinor Ostrom')
  await yearFromInput.fill('1990')

  await page.evaluate(() => { window.location.hash = '#/admin' })
  const apiKey = page.locator('.admin-settings-form input[type="password"]').first()
  await expect(apiKey).toBeVisible()
  await apiKey.fill('draft-secret')
  expect(settingsRequests).toBe(1)

  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  // The sign-in lands on the workspace once its loads are done (the log
  // socket connects right before); open the Admin tab after that.
  await expect.poll(() => sockets.opened).toBe(2)
  await expect(page.getByTestId('seed-panel')).toBeVisible()

  await page.getByRole('button', { name: 'Admin', exact: true }).click()
  // The draft is gone and the Admin tab fetched the settings again.
  await expect(apiKey).toBeVisible()
  await expect(apiKey).toHaveValue('')
  expect(settingsRequests).toBe(2)

  await page.evaluate(() => { window.location.hash = '#/workspace' })
  await expect(author).toBeVisible()
  await expect(author).toHaveValue('')
  await expect(yearFromInput).toHaveValue('')
})

test('a corpus switch whose /me answers after a re-login does not restore the old user', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  const corpusTwo = { id: 2, name: 'Second corpus', role: 'owner', owner_username: 'alice' }
  let holdNextMe = false
  let releaseHeldMe: () => void = () => {}
  const heldMeReleased = new Promise<void>((resolve) => { releaseHeldMe = resolve })
  let heldMeSeen = false
  let heldMeAnswered = false
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: userB })
    if (path === '/api/corpora/2/select') {
      holdNextMe = true
      return fulfil(route, {})
    }
    if (path === '/api/auth/me') {
      if (auth.endsWith('token-b')) return fulfil(route, { user: userB, corpora: [corpus] })
      if (holdNextMe) {
        holdNextMe = false
        heldMeSeen = true
        await heldMeReleased
        await fulfil(route, { user: { ...userA, last_corpus_id: 2 }, corpora: [corpus, corpusTwo] })
        heldMeAnswered = true
        return
      }
      return fulfil(route, { user: userA, corpora: [corpus, corpusTwo] })
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await expect(page.getByTestId('auth-user')).toHaveText('alice')
  await page.locator('.header-corpus-picker select').selectOption('2')
  await expect.poll(() => heldMeSeen).toBe(true)
  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  await page.evaluate(() => {
    const seen: string[] = []
    ;(window as unknown as { __authUserSeen: string[] }).__authUserSeen = seen
    new MutationObserver(() => {
      const text = document.querySelector('[data-testid="auth-user"]')?.textContent || ''
      if (seen[seen.length - 1] !== text) seen.push(text)
    }).observe(document.body, { subtree: true, childList: true, characterData: true })
  })

  releaseHeldMe()
  await expect.poll(() => heldMeAnswered).toBe(true)
  await page.waitForTimeout(500)
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  const seen = await page.evaluate(() => (window as unknown as { __authUserSeen: string[] }).__authUserSeen)
  expect(seen).not.toContain('alice')
})

test('a settings load that fails after a sign-out does not show its error to the next user', async ({ page }) => {
  const sockets = { opened: 0, closed: 0 }
  const adminA = { ...userA, is_admin: true }
  const adminB = { ...userB, is_admin: true }
  let releaseHeld: () => void = () => {}
  const held = new Promise<void>((resolve) => { releaseHeld = resolve })
  let heldSeen = false
  let heldAnswered = false
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockSocket(page, sockets)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: adminB })
    if (path === '/api/auth/me') {
      return fulfil(route, { user: auth.endsWith('token-b') ? adminB : adminA, corpora: [corpus] })
    }
    if (path === '/api/admin/settings') {
      if (auth.endsWith('token-a')) {
        heldSeen = true
        await held
        await fulfil(route, { error: 'Stale settings failure' }, 500)
        heldAnswered = true
        return
      }
      return fulfil(route, { settings: { openalex_api_key: { is_set: false } } })
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/admin')
  await expect(page.getByTestId('auth-user')).toHaveText('alice')
  await expect.poll(() => heldSeen).toBe(true)
  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  await expect.poll(() => sockets.opened).toBe(2)
  await expect(page.getByTestId('seed-panel')).toBeVisible()

  releaseHeld()
  await expect.poll(() => heldAnswered).toBe(true)
  await page.waitForTimeout(300)
  await page.getByRole('button', { name: 'Admin', exact: true }).click()
  await expect(page.locator('.admin-settings-form')).toBeVisible()
  await expect(page.getByText('Stale settings failure')).toHaveCount(0)
  await expect(page.getByText('Failed to load settings')).toHaveCount(0)
})

test('a network failure and a timeout reach the login form as readable text', async ({ page }) => {
  let mode: 'network' | 'hang' = 'network'
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/login') {
      if (mode === 'network') return route.abort('connectionfailed')
      return // Never answered: the request timeout has to end it.
    }
    return fulfil(route, { error: 'Unauthorized' }, 401)
  })

  await page.goto('/#/workspace')
  await logIn(page, 'alice')
  const error = page.getByTestId('login-form').locator('.error')
  await expect(error).toHaveText('Could not reach the server.')

  mode = 'hang'
  await logIn(page, 'alice')
  await expect(error).toHaveText('The server did not answer in time.', { timeout: 10_000 })
})

test('the request timeout also covers reading the response body', async ({ page }) => {
  // A real stall: the headers and half a body arrive, the rest never does.
  const server = createServer((_request, response) => {
    response.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' })
    response.write('{"corpora":')
  })
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const stallUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/stall`
  try {
    await mockLogSocket(page)
    await page.route('**/api/**', (route) => fulfil(route, { error: 'Unauthorized' }, 401))
    await page.goto('/#/workspace')
    await expect(page.getByTestId('login-form')).toBeVisible()
    const outcome = await page.evaluate(async (url) => {
      const realFetch = window.fetch
      window.fetch = (_input, options) => realFetch(url, options)
      try {
        const api = await import('/src/lib/api.js' as string)
        await api.fetchCorpora()
        return 'resolved'
      } catch (error) {
        return (error as Error).message
      } finally {
        window.fetch = realFetch
      }
    }, stallUrl)
    expect(outcome).toBe('The server did not answer in time.')
  } finally {
    server.closeAllConnections()
    server.close()
  }
})

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((r) => { resolve = r })
  return { promise, resolve }
}

const signInLinks = [
  { flow: 'accept-invite', endpoint: '/api/auth/accept-invite', button: 'Activate account' },
  { flow: 'reset-password', endpoint: '/api/auth/reset-password', button: 'Save password' },
]

async function submitPassword(page: Page, button: string) {
  await page.getByLabel('New password').fill('secret-password')
  await page.getByLabel('Confirm password').fill('secret-password')
  await page.getByRole('button', { name: button }).click()
}

for (const { flow, endpoint, button } of signInLinks) {
  test(`${flow} completed over a live session runs the full sign-out reset`, async ({ page }) => {
    const sockets = { opened: 0, closed: 0 }
    const logSockets: WebSocketRoute[] = []
    const heldSeeds = deferred()
    const seedAnswerReleased = deferred()
    let holdSeedsForA = false
    let seedsHeld = false
    let seedRequestsForB = 0
    const bobSeed = { ...seedSource, id: 'keyword:bob', source_key: 'bob', label: 'Bob Seed' }
    await page.addInitScript(() => {
      if (!window.sessionStorage.getItem('booted')) {
        window.sessionStorage.setItem('booted', '1')
        window.localStorage.setItem('rag_feeder_token', 'token-a')
      }
    })
    await mockLogSocket(page, (ws) => {
      sockets.opened += 1
      logSockets.push(ws)
      ws.onClose(() => { sockets.closed += 1 })
    })
    await page.route('**/api/**', async (route) => {
      const request = route.request()
      const path = new URL(request.url()).pathname
      const auth = request.headers().authorization || ''
      if (path === endpoint) return fulfil(route, { token: 'token-b', user: userB })
      if (path === '/api/auth/me') {
        return fulfil(route, { user: auth.endsWith('token-b') ? userB : userA, corpora: [corpus] })
      }
      if (path === '/api/seed/sources') {
        if (auth.endsWith('token-b')) {
          seedRequestsForB += 1
          return fulfil(route, { sources: [bobSeed], total: 1, source: 'api' })
        }
        if (holdSeedsForA) {
          seedsHeld = true
          heldSeeds.resolve()
          await seedAnswerReleased.promise
        }
      }
      return workspaceDefaults(path, route)
    })

    await page.goto('/#/workspace')
    await expect(page.getByTestId('auth-user')).toHaveText('alice')
    await page.getByText('Seed One').click()
    await expect(page.getByText('Candidate Alpha')).toBeVisible()
    await expect.poll(() => sockets.opened).toBe(1)
    logSockets[0].send('[worker] line of alice')

    // A's seed load is in flight when the link for B completes.
    holdSeedsForA = true
    await page.evaluate(() => window.dispatchEvent(new Event('focus')))
    await heldSeeds.promise
    expect(seedsHeld).toBe(true)
    await page.evaluate((hash) => { window.location.hash = hash }, `#/${flow}?token=one-time`)
    await submitPassword(page, button)

    await expect(page.getByTestId('auth-user')).toHaveText('bob')
    // B's load started fresh instead of joining A's.
    await expect.poll(() => seedRequestsForB).toBeGreaterThanOrEqual(1)
    await expect(page.getByText('Bob Seed')).toBeVisible()
    seedAnswerReleased.resolve()
    await page.waitForTimeout(300)
    await expect(page.getByText('Bob Seed')).toBeVisible()
    await expect(page.getByText('Seed One')).toHaveCount(0)
    await expect(page.getByText('Candidate Alpha')).toHaveCount(0)
    // A's socket was closed and B has its own, with A's lines gone.
    await expect.poll(() => sockets.closed).toBe(1)
    await expect.poll(() => sockets.opened).toBe(2)
    await page.evaluate(() => { window.location.hash = '#/logs' })
    await expect(page.getByText('line of alice')).toHaveCount(0)
  })
}

test('a sign-in whose own /me answers 401 shows the error on the login form', async ({ page }) => {
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-x', user: userA })
    return fulfil(route, { error: 'Unknown user' }, 401)
  })
  await page.goto('/#/workspace')
  await logIn(page, 'alice')
  await expect(page.getByTestId('login-form').locator('.error')).toHaveText('Unknown user')
  expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBeNull()
})

for (const { flow, endpoint, button } of signInLinks) {
  test(`${flow} whose own /me answers 401 shows the saved-password error on the sign-in form`, async ({ page }) => {
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const path = new URL(route.request().url()).pathname
      if (path === endpoint) return fulfil(route, { token: 'token-x', user: userA })
      return fulfil(route, { error: 'Unknown user' }, 401)
    })
    await page.goto(`/#/${flow}?token=one-time`)
    await submitPassword(page, button)
    // The password call succeeded, so the failed /me is a failed sign-in (D12).
    await expect(page.locator('.login-form .error'))
      .toHaveText('Your password was saved, but signing in failed (Unknown user). Sign in with your new password.')
  })

  test(`a stale-token 401 at boot does not discard an in-flight ${flow}`, async ({ page }) => {
    const bootAnswer = deferred()
    const linkAnswer = deferred()
    let linkSeen = false
    await page.addInitScript(() => {
      if (!window.sessionStorage.getItem('booted')) {
        window.sessionStorage.setItem('booted', '1')
        window.localStorage.setItem('rag_feeder_token', 'stale-token')
      }
    })
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const request = route.request()
      const path = new URL(request.url()).pathname
      const auth = request.headers().authorization || ''
      if (path === endpoint) {
        linkSeen = true
        await linkAnswer.promise
        return fulfil(route, { token: 'token-b', user: userB })
      }
      if (auth.endsWith('stale-token')) {
        await bootAnswer.promise
        return fulfil(route, { error: 'Invalid token' }, 401)
      }
      if (path === '/api/auth/me') return fulfil(route, { user: userB, corpora: [corpus] })
      return workspaceDefaults(path, route)
    })

    await page.goto(`/#/${flow}?token=one-time`)
    await submitPassword(page, button)
    await expect.poll(() => linkSeen).toBe(true)
    // The expired token's 401 lands while the submit is in flight.
    bootAnswer.resolve()
    await page.waitForFunction(() => !window.localStorage.getItem('rag_feeder_token'))
    linkAnswer.resolve()
    await expect(page.getByTestId('auth-user')).toHaveText('bob')
    expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBe('token-b')
  })

  test(`a stale-token 401 at boot leaves the failure of ${flow} on the form`, async ({ page }) => {
    const bootAnswer = deferred()
    await page.addInitScript(() => {
      window.localStorage.setItem('rag_feeder_token', 'stale-token')
    })
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const path = new URL(route.request().url()).pathname
      if (path === endpoint) return fulfil(route, { error: 'Link is invalid or expired' }, 400)
      await bootAnswer.promise
      return fulfil(route, { error: 'Invalid token' }, 401)
    })

    await page.goto(`/#/${flow}?token=one-time`)
    await submitPassword(page, button)
    const error = page.locator('.login-form .error')
    await expect(error).toHaveText('Link is invalid or expired')
    bootAnswer.resolve()
    await page.waitForFunction(() => !window.localStorage.getItem('rag_feeder_token'))
    await page.waitForTimeout(100)
    await expect(error).toHaveText('Link is invalid or expired')
  })
}

test('a stale-token 401 at boot does not discard an in-flight login', async ({ page }) => {
  const bootAnswer = deferred()
  const loginAnswer = deferred()
  let loginSeen = false
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'stale-token')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') {
      loginSeen = true
      await loginAnswer.promise
      return fulfil(route, { token: 'token-b', user: userB })
    }
    if (auth.endsWith('stale-token')) {
      await bootAnswer.promise
      return fulfil(route, { error: 'Invalid token' }, 401)
    }
    if (path === '/api/auth/me') return fulfil(route, { user: userB, corpora: [corpus] })
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await logIn(page, 'bob')
  await expect.poll(() => loginSeen).toBe(true)
  bootAnswer.resolve()
  await page.waitForFunction(() => !window.localStorage.getItem('rag_feeder_token'))
  loginAnswer.resolve()
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  expect(await page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))).toBe('token-b')
})

test('a 401 on a search preview stops the search and leaves no status behind', async ({ page }) => {
  let expired = false
  let searchPosts = 0
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: userB })
    if (path === '/api/auth/me') return fulfil(route, { user: userA, corpora: [corpus] })
    if (path === '/api/keyword-search') {
      searchPosts += 1
      return fulfil(route, { error: 'Unauthorized' }, 401)
    }
    if (path === '/api/keyword-search/preview') {
      expired = true
      return fulfil(route, { error: 'Unauthorized' }, 401)
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('textbox', { name: 'Query' }).fill('economics')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  await expect(page.getByTestId('login-form')).toBeVisible()
  expect(expired).toBe(true)
  await logIn(page, 'alice')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await expect(card.locator('.seed-search-footer p.muted').first()).toHaveText('')
  expect(searchPosts).toBe(0)
})

test('a search preview answered after a sign-out does not reach the next user', async ({ page }) => {
  const previewAnswer = deferred()
  let previewSeen = false
  let searchPosts = 0
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: userB })
    if (path === '/api/auth/me') {
      return fulfil(route, { user: auth.endsWith('token-b') ? userB : userA, corpora: [corpus] })
    }
    if (path === '/api/keyword-search') {
      searchPosts += 1
      return fulfil(route, { runId: 1, status: 'running' }, 202)
    }
    if (path === '/api/keyword-search/preview') {
      previewSeen = true
      await previewAnswer.promise
      return fulfil(route, { count: 5, threshold: 100000 })
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  const card = page.locator('.seed-intake-card--search')
  await card.getByRole('textbox', { name: 'Query' }).fill('economics')
  await card.getByRole('button', { name: 'Search', exact: true }).click()
  await expect.poll(() => previewSeen).toBe(true)
  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')

  previewAnswer.resolve()
  await page.waitForTimeout(300)
  await expect(page.getByText(/works match/)).toHaveCount(0)
  expect(searchPosts).toBe(0)
})

test('an invitation that answers 401 leaves no status for the next admin', async ({ page }) => {
  const adminA = { ...userA, is_admin: true }
  const adminB = { ...userB, is_admin: true }
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: adminB })
    if (path === '/api/auth/me') {
      return fulfil(route, { user: auth.endsWith('token-b') ? adminB : adminA, corpora: [corpus] })
    }
    if (path === '/api/auth/invitations') return fulfil(route, { error: 'Invalid token' }, 401)
    if (path === '/api/admin/settings') return fulfil(route, { settings: {} })
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/admin')
  const form = page.locator('.admin-invite-form')
  await form.getByLabel('Username').fill('carol')
  await form.getByLabel('Email').fill('carol@example.org')
  await form.getByRole('button', { name: /invit/i }).click()
  await expect(page.getByTestId('login-form')).toBeVisible()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  // The sign-in lands on the workspace once its loads finish. Until then the
  // hash is still the signed-out '#/admin', so setting it again is no
  // navigation at all and the landing would win: wait for it first.
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.evaluate(() => { window.location.hash = '#/admin' })
  await expect(page.locator('.admin-invite-form')).toBeVisible()
  await expect(page.locator('.admin-invite-form__status')).toHaveCount(0)
})

test('the log type and corpus filter go back to their defaults on sign-out', async ({ page }) => {
  const adminA = { ...userA, is_admin: true }
  const adminB = { ...userB, is_admin: true }
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: adminB })
    if (path === '/api/auth/me') {
      return fulfil(route, { user: auth.endsWith('token-b') ? adminB : adminA, corpora: [corpus] })
    }
    if (path === '/api/logs/tail') return fulfil(route, { lines: [] })
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/logs')
  const type = page.getByTestId('logs-type-select')
  const currentCorpus = page.getByRole('checkbox', { name: 'Current corpus only' })
  await type.selectOption('app')
  await currentCorpus.uncheck()
  await expect(type).toHaveValue('app')

  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('auth-user')).toHaveText('bob')
  // As above: the hash is still '#/logs' until the sign-in lands.
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.evaluate(() => { window.location.hash = '#/logs' })
  await expect(type).toHaveValue('pipeline')
  await expect(currentCorpus).toBeChecked()
})

const EXPIRED = 'Your session has expired. Please sign in again.'

function storedToken(page: Page) {
  return page.evaluate(() => window.localStorage.getItem('rag_feeder_token'))
}

test('a 502 at boot keeps the token and offers Retry', async ({ page }) => {
  let meRequests = 0
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') {
      meRequests += 1
      if (meRequests === 1) return fulfil(route, { error: 'Bad gateway' }, 502)
      return fulfil(route, { user: userA, corpora: [corpus] })
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  const form = page.getByTestId('login-form')
  await expect(form.locator('.error')).toHaveText('Bad gateway. Your session is kept; press Retry when the server is back.')
  expect(await storedToken(page)).toBe('token-a')
  await page.waitForTimeout(500)
  // No automatic retry.
  expect(meRequests).toBe(1)
  await page.getByTestId('auth-retry').click()
  await expect(page.getByTestId('auth-user')).toHaveText('alice')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  expect(await storedToken(page)).toBe('token-a')
})

test('a network failure at boot keeps the token too', async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'token-a')
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') return route.abort('connectionfailed')
    return workspaceDefaults(path, route)
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('login-form').locator('.error'))
    .toHaveText('Could not reach the server. Your session is kept; press Retry when the server is back.')
  await expect(page.getByTestId('auth-retry')).toBeVisible()
  expect(await storedToken(page)).toBe('token-a')
})

test('a 401 mid-session says the session expired', async ({ page }) => {
  let expired = false
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'token-a')
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (expired) return fulfil(route, { error: 'Unauthorized' }, 401)
    if (path === '/api/auth/me') return fulfil(route, { user: userA, corpora: [corpus] })
    return workspaceDefaults(path, route)
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  expired = true
  await page.evaluate(() => window.dispatchEvent(new Event('focus')))
  await expect(page.getByTestId('login-form').locator('.error')).toHaveText(EXPIRED)
  await expect(page.getByTestId('auth-retry')).toHaveCount(0)
})

test('a 4401 socket close says the session expired', async ({ page }) => {
  await page.addInitScript(() => {
    window.localStorage.setItem('rag_feeder_token', 'token-a')
  })
  await mockLogSocket(page, (ws) => { setTimeout(() => ws.close({ code: 4401 }), 200) })
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/me') return fulfil(route, { user: userA, corpora: [corpus] })
    return workspaceDefaults(path, route)
  })
  await page.goto('/#/workspace')
  await expect(page.getByTestId('login-form').locator('.error')).toHaveText(EXPIRED)
  expect(await storedToken(page)).toBeNull()
})

test('a stale token at boot says the session expired; an explicit sign-out says nothing', async ({ page }) => {
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'stale-token')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: userB })
    if (!auth.endsWith('token-b')) return fulfil(route, { error: 'Invalid token' }, 401)
    if (path === '/api/auth/me') return fulfil(route, { user: userB, corpora: [corpus] })
    return workspaceDefaults(path, route)
  })
  await page.goto('/#/workspace')
  const form = page.getByTestId('login-form')
  await expect(form.locator('.error')).toHaveText(EXPIRED)
  await logIn(page, 'bob')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await page.getByRole('button', { name: 'Log out' }).click()
  await expect(form).toBeVisible()
  await page.waitForTimeout(300)
  await expect(form.locator('.error')).toHaveCount(0)
})

test('a stale boot 401 during a sign-in submit leaves the submit outcome on the form', async ({ page }) => {
  const bootAnswer = deferred()
  const loginAnswer = deferred()
  let loginSeen = false
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'stale-token')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') {
      loginSeen = true
      await loginAnswer.promise
      return fulfil(route, { error: 'Invalid credentials' }, 401)
    }
    if (auth.endsWith('stale-token')) {
      await bootAnswer.promise
      return fulfil(route, { error: 'Invalid token' }, 401)
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await logIn(page, 'bob')
  await expect.poll(() => loginSeen).toBe(true)
  bootAnswer.resolve()
  await page.waitForFunction(() => !window.localStorage.getItem('rag_feeder_token'))
  const error = page.getByTestId('login-form').locator('.error')
  for (let i = 0; i < 6; i += 1) {
    await expect(page.getByText(EXPIRED)).toHaveCount(0)
    await page.waitForTimeout(50)
  }
  loginAnswer.resolve()
  await expect(error).toHaveText('Invalid credentials')
  await page.waitForTimeout(300)
  await expect(error).toHaveText('Invalid credentials')
})

for (const { flow, endpoint, button } of signInLinks) {
  test(`${flow} accepted but /me fails: sign-in form with the username and the saved-password text`, async ({ page }) => {
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const path = new URL(route.request().url()).pathname
      if (path === endpoint) return fulfil(route, { token: 'token-new', user: { id: 7, username: 'neu', last_corpus_id: 1, is_admin: false } })
      if (path === '/api/auth/me') return fulfil(route, { error: 'boom' }, 500)
      return workspaceDefaults(path, route)
    })
    await page.goto(`/#/${flow}?token=t`)
    await submitPassword(page, button)
    const form = page.getByTestId('login-form')
    await expect(form).toBeVisible()
    await expect(form.getByLabel('Username')).toHaveValue('neu')
    await expect(form.locator('.error'))
      .toHaveText('Your password was saved, but signing in failed (boom). Sign in with your new password.')
    expect(await storedToken(page)).toBeNull()
    expect(await page.evaluate(() => window.location.hash)).toBe('#/login')
  })
}

test('a quota answer from before a sign-out does not reach the next session', async ({ page }) => {
  const oldAnswer = deferred()
  let oldSeen = false
  let oldAnswered = false
  let bobQuotaRequests = 0
  const quota = (remaining: number) => ({ available: true, stale: false, api_key_present: true, remaining, limit: 1000, reset_in_seconds: null })
  await page.addInitScript(() => {
    if (!window.sessionStorage.getItem('booted')) {
      window.sessionStorage.setItem('booted', '1')
      window.localStorage.setItem('rag_feeder_token', 'token-a')
    }
  })
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: userB })
    if (path === '/api/auth/me') {
      return fulfil(route, { user: auth.endsWith('token-b') ? userB : userA, corpora: [corpus] })
    }
    if (path === '/api/openalex/quota') {
      if (auth.endsWith('token-b')) {
        bobQuotaRequests += 1
        return fulfil(route, quota(222))
      }
      oldSeen = true
      await oldAnswer.promise
      await fulfil(route, quota(111))
      oldAnswered = true
      return
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await expect(page.getByTestId('auth-user')).toHaveText('alice')
  await expect.poll(() => oldSeen).toBe(true)
  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  // The old request still in flight did not block bob's own.
  await expect.poll(() => bobQuotaRequests).toBeGreaterThanOrEqual(1)
  const pill = page.getByTestId('openalex-quota')
  await expect(pill).toContainText('222 of 1,000')
  oldAnswer.resolve()
  await expect.poll(() => oldAnswered).toBe(true)
  await page.waitForTimeout(300)
  await expect(pill).toContainText('222 of 1,000')
})

for (const outcome of ['success', 'failure'] as const) {
  test(`a settings save that settles (${outcome}) after a sign-out writes nothing for the next admin`, async ({ page }) => {
    const adminA = { ...userA, is_admin: true }
    const adminB = { ...userB, is_admin: true }
    const saveAnswer = deferred()
    let saveSeen = false
    let saveAnswered = false
    await page.addInitScript(() => {
      if (!window.sessionStorage.getItem('booted')) {
        window.sessionStorage.setItem('booted', '1')
        window.localStorage.setItem('rag_feeder_token', 'token-a')
      }
    })
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const request = route.request()
      const path = new URL(request.url()).pathname
      const auth = request.headers().authorization || ''
      if (path === '/api/auth/login') return fulfil(route, { token: 'token-b', user: adminB })
      if (path === '/api/auth/me') {
        return fulfil(route, { user: auth.endsWith('token-b') ? adminB : adminA, corpora: [corpus] })
      }
      if (path === '/api/admin/settings') {
        if (request.method() === 'PUT') {
          saveSeen = true
          await saveAnswer.promise
          if (outcome === 'success') await fulfil(route, { ok: true })
          else await fulfil(route, { error: 'Stale save failure' }, 500)
          saveAnswered = true
          return
        }
        return fulfil(route, { settings: { openalex_api_key: { is_set: false } } })
      }
      return workspaceDefaults(path, route)
    })

    await page.goto('/#/admin')
    await page.locator('.admin-settings-form').getByRole('button', { name: 'Save settings' }).click()
    await expect.poll(() => saveSeen).toBe(true)
    await page.getByRole('button', { name: 'Log out' }).click()
    await logIn(page, 'bob')
    await expect(page.getByTestId('auth-user')).toHaveText('bob')
    await expect(page.getByTestId('seed-panel')).toBeVisible()
    await page.evaluate(() => { window.location.hash = '#/admin' })
    const form = page.locator('.admin-settings-form')
    await expect(form).toBeVisible()
    saveAnswer.resolve()
    await expect.poll(() => saveAnswered).toBe(true)
    await page.waitForTimeout(300)
    await expect(form.getByText('Settings saved', { exact: false })).toHaveCount(0)
    await expect(form.getByText('Stale save failure')).toHaveCount(0)
  })
}

for (const { flow, endpoint, button } of signInLinks) {
  test(`${flow} whose workspace load answers 401 after /me still shows the saved-password text`, async ({ page }) => {
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const request = route.request()
      const path = new URL(request.url()).pathname
      const auth = request.headers().authorization || ''
      if (path === endpoint) return fulfil(route, { token: 'token-new', user: { id: 7, username: 'neu', last_corpus_id: 1, is_admin: false } })
      if (!auth.endsWith('token-new')) return fulfil(route, { error: 'Unauthorized' }, 401)
      if (path === '/api/auth/me') return fulfil(route, { user: { id: 7, username: 'neu', last_corpus_id: 1, is_admin: false }, corpora: [corpus] })
      // The new token is rejected by the first workspace load after /me.
      if (path === '/api/corpus') return fulfil(route, { error: 'Invalid token' }, 401)
      return workspaceDefaults(path, route)
    })
    await page.goto(`/#/${flow}?token=t`)
    await submitPassword(page, button)
    const form = page.getByTestId('login-form')
    await expect(form).toBeVisible()
    await expect(form.getByLabel('Username')).toHaveValue('neu')
    await expect(form.locator('.error'))
      .toHaveText('Your password was saved, but signing in failed (Invalid token). Sign in with your new password.')
    expect(await storedToken(page)).toBeNull()
  })
}

test('a sign-in abandoned by a sign-out does not suppress the next session\'s expiry message', async ({ page }) => {
  const aliceConfig = deferred()
  let aliceConfigSeen = false
  let bobExpired = false
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const path = new URL(request.url()).pathname
    const auth = request.headers().authorization || ''
    if (path === '/api/auth/login') {
      const { username } = request.postDataJSON()
      return username === 'bob'
        ? fulfil(route, { token: 'token-b', user: userB })
        : fulfil(route, { token: 'token-a', user: userA })
    }
    if (auth.endsWith('token-b') && bobExpired) return fulfil(route, { error: 'Unauthorized' }, 401)
    if (!auth) return fulfil(route, { error: 'Unauthorized' }, 401)
    if (path === '/api/auth/me') return fulfil(route, { user: auth.endsWith('token-b') ? userB : userA, corpora: [corpus] })
    if (path === '/api/recursion-config' && auth.endsWith('token-a')) {
      aliceConfigSeen = true
      await aliceConfig.promise
    }
    return workspaceDefaults(path, route)
  })

  await page.goto('/#/workspace')
  await logIn(page, 'alice')
  await expect.poll(() => aliceConfigSeen).toBe(true)
  // Alice's sign-in is still loading when she signs out.
  await page.getByRole('button', { name: 'Log out' }).click()
  await logIn(page, 'bob')
  await expect(page.getByTestId('seed-panel')).toBeVisible()
  await expect(page.getByTestId('auth-user')).toHaveText('bob')

  bobExpired = true
  await page.evaluate(() => window.dispatchEvent(new Event('focus')))
  await expect(page.getByTestId('login-form').locator('.error')).toHaveText(EXPIRED)
  aliceConfig.resolve()
})

for (const { flow, endpoint, button } of signInLinks) {
  test(`a delayed ${flow} recovery does not overwrite a newer failed sign-in`, async ({ page }) => {
    const heldSeeds = deferred()
    let seedsHeld = false
    let seedsAnswered = false
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const request = route.request()
      const path = new URL(request.url()).pathname
      const auth = request.headers().authorization || ''
      if (path === endpoint) return fulfil(route, { token: 'token-new', user: { id: 7, username: 'neu', last_corpus_id: 1, is_admin: false } })
      if (path === '/api/auth/login') return fulfil(route, { error: 'Invalid credentials' }, 401)
      if (!auth.endsWith('token-new')) return fulfil(route, { error: 'Unauthorized' }, 401)
      if (path === '/api/auth/me') return fulfil(route, { user: { id: 7, username: 'neu', last_corpus_id: 1, is_admin: false }, corpora: [corpus] })
      if (path === '/api/seed/sources') {
        seedsHeld = true
        await heldSeeds.promise
        await fulfil(route, { sources: [], total: 0, source: 'api' })
        seedsAnswered = true
        return
      }
      if (path === '/api/corpus') return fulfil(route, { error: 'Invalid token' }, 401)
      return workspaceDefaults(path, route)
    })
    await page.goto(`/#/${flow}?token=t`)
    await submitPassword(page, button)
    await expect.poll(() => seedsHeld).toBe(true)
    const form = page.getByTestId('login-form')
    await expect(form).toBeVisible()
    await expect.poll(() => storedToken(page)).toBeNull()

    // A newer sign-in, which fails, owns the form from here on.
    await form.getByLabel('Username').fill('carol')
    await form.getByLabel('Password').fill('wrong-password')
    await form.getByRole('button', { name: 'Sign in' }).click()
    await expect(form.locator('.error')).toHaveText('Invalid credentials')

    heldSeeds.resolve()
    await expect.poll(() => seedsAnswered).toBe(true)
    await page.waitForTimeout(300)
    await expect(form.locator('.error')).toHaveText('Invalid credentials')
    await expect(form.getByLabel('Username')).toHaveValue('carol')
  })
}

test('a second sign-in submit while one is in flight is ignored', async ({ page }) => {
  const firstAnswer = deferred()
  let loginRequests = 0
  await mockLogSocket(page)
  await page.route('**/api/**', async (route) => {
    const path = new URL(route.request().url()).pathname
    if (path === '/api/auth/login') {
      loginRequests += 1
      if (loginRequests === 1) {
        await firstAnswer.promise
        return fulfil(route, { error: 'Invalid credentials' }, 401)
      }
      // A duplicate would answer later with a server error.
      return fulfil(route, { error: 'Bad gateway' }, 502)
    }
    return fulfil(route, { error: 'Unauthorized' }, 401)
  })
  await page.goto('/#/workspace')
  const form = page.getByTestId('login-form')
  await logIn(page, 'alice')
  await expect.poll(() => loginRequests).toBe(1)
  await expect(form.getByRole('button', { name: 'Sign in' })).toBeDisabled()
  // Enter in a field (the form's own submit) bypasses the button.
  await form.evaluate((element: HTMLFormElement) => element.requestSubmit())
  await page.waitForTimeout(200)
  expect(loginRequests).toBe(1)
  firstAnswer.resolve()
  await expect(form.locator('.error')).toHaveText('Invalid credentials')
  await page.waitForTimeout(300)
  await expect(form.locator('.error')).toHaveText('Invalid credentials')
  expect(loginRequests).toBe(1)
  await expect(form.getByRole('button', { name: 'Sign in' })).toBeEnabled()
})

for (const { flow, endpoint, button } of signInLinks) {
  test(`a second ${flow} submit while one is in flight is ignored and leaves no token`, async ({ page }) => {
    const firstAnswer = deferred()
    let linkRequests = 0
    await mockLogSocket(page)
    await page.route('**/api/**', async (route) => {
      const path = new URL(route.request().url()).pathname
      if (path === endpoint) {
        linkRequests += 1
        if (linkRequests === 1) {
          await firstAnswer.promise
          return fulfil(route, { token: 'token-new', user: { id: 7, username: 'neu', last_corpus_id: 1, is_admin: false } })
        }
        return fulfil(route, { error: 'Link is invalid or expired' }, 400)
      }
      if (path === '/api/auth/me') return fulfil(route, { error: 'boom' }, 500)
      return workspaceDefaults(path, route)
    })
    await page.goto(`/#/${flow}?token=t`)
    await submitPassword(page, button)
    await expect.poll(() => linkRequests).toBe(1)
    const submitButton = page.getByRole('button', { name: button })
    await expect(submitButton).toBeDisabled()
    await page.locator('.login-form').evaluate((element: HTMLFormElement) => element.requestSubmit())
    await page.waitForTimeout(200)
    expect(linkRequests).toBe(1)
    firstAnswer.resolve()
    const form = page.getByTestId('login-form')
    await expect(form.locator('.error'))
      .toHaveText('Your password was saved, but signing in failed (boom). Sign in with your new password.')
    await expect(form.getByLabel('Username')).toHaveValue('neu')
    expect(await storedToken(page)).toBeNull()
    expect(linkRequests).toBe(1)
  })
}
