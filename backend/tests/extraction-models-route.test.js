// Route-level proof that /api/extract-bibliography hands the Admin-tab inline
// models (then env, then defaults) to the APIscraper children. A fake
// interpreter stands in for Python and records every child it is asked to run.
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import request from 'supertest'

const FAKE_PYTHON = `#!/usr/bin/env node
const fs = require('node:fs')
const path = require('node:path')
const env = process.env
const script = path.basename(process.argv[2] || '')
const args = process.argv.slice(3)
fs.appendFileSync(env.FAKE_PY_LOG, JSON.stringify({
  script,
  args,
  inline: env.RAG_FEEDER_API_INLINE_MODEL ?? null,
  extract: env.RAG_FEEDER_API_EXTRACT_MODEL ?? null,
}) + '\\n')
const argAfter = (flag) => {
  const i = args.indexOf(flag)
  return i >= 0 ? args[i + 1] : null
}
if (script === 'pdf_page_count.py') {
  console.log('3')
} else if (script === 'llm_upload_pdf.py') {
  console.log(JSON.stringify({ name: 'files/fake-upload' }))
} else if (script === 'llm_delete_upload.py') {
  console.log(JSON.stringify({ deleted: true }))
  // The route's last child: the test waits for this before the next step.
  fs.appendFileSync(env.FAKE_PY_LOG, JSON.stringify({ script, finished: true }) + '\\n')
} else if (script === 'get_bib_pages.py') {
  const code = Number(env.FAKE_GET_BIB_EXIT ?? '1')
  if (code === 0) {
    const input = argAfter('--input-pdf')
    const base = path.basename(input, path.extname(input))
    const dir = path.join(argAfter('--output-dir'), base)
    fs.mkdirSync(dir, { recursive: true })
    fs.writeFileSync(path.join(dir, 'page_1.pdf'), '%PDF-1.4\\n%%EOF\\n')
  }
  process.exit(code)
} else if (script === 'APIscraper_v2.py') {
  console.log('fake APIscraper: no entries')
}
process.exit(0)
`

describe('/api/extract-bibliography inline models', () => {
  let app
  let token = ''
  let tmpDir = ''
  let uploadsDir = ''
  let logPath = ''
  const events = []
  const saved = {}
  const ENV_KEYS = [
    'RAG_FEEDER_PYTHON',
    'RAG_FEEDER_UPLOADS_DIR',
    'RAG_FEEDER_ARTIFACTS_DIR',
    'FAKE_PY_LOG',
    'FAKE_GET_BIB_EXIT',
    'RAG_FEEDER_API_INLINE_MODEL',
    'RAG_FEEDER_API_INLINE_CHEAP_MODEL',
    'RAG_FEEDER_JWT_SECRET',
    'RAG_ADMIN_USER',
    'RAG_ADMIN_PASSWORD',
  ]

  beforeAll(async () => {
    for (const key of ENV_KEYS) saved[key] = process.env[key]
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'dt-extraction-models-'))
    uploadsDir = path.join(tmpDir, 'uploads')
    fs.mkdirSync(uploadsDir)
    logPath = path.join(tmpDir, 'fake-python.log')
    const fakePython = path.join(tmpDir, 'fake-python')
    fs.writeFileSync(fakePython, FAKE_PYTHON)
    fs.chmodSync(fakePython, 0o755)
    process.env.RAG_FEEDER_PYTHON = fakePython
    process.env.RAG_FEEDER_UPLOADS_DIR = uploadsDir
    process.env.RAG_FEEDER_ARTIFACTS_DIR = path.join(tmpDir, 'artifacts')
    process.env.FAKE_PY_LOG = logPath
    delete process.env.RAG_FEEDER_API_INLINE_MODEL
    delete process.env.RAG_FEEDER_API_INLINE_CHEAP_MODEL
    process.env.RAG_FEEDER_JWT_SECRET = 'extraction-models-secret'
    process.env.RAG_ADMIN_USER = 'extraction-models-admin'
    process.env.RAG_ADMIN_PASSWORD = 'extraction-models-password'
    const { createApp } = await import('../src/app.js')
    app = createApp({ broadcast() {}, broadcastEvent: (event) => events.push(event) })
    const login = await request(app).post('/api/auth/login').send({
      username: 'extraction-models-admin',
      password: 'extraction-models-password',
    })
    expect(login.status).toBe(200)
    token = login.body.token
  })

  afterAll(() => {
    for (const key of ENV_KEYS) {
      if (saved[key] === undefined) delete process.env[key]
      else process.env[key] = saved[key]
    }
    fs.rmSync(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    fs.writeFileSync(logPath, '')
    events.length = 0
  })

  const putSettings = async (settings) => {
    const res = await request(app)
      .put('/api/admin/settings')
      .set('Authorization', `Bearer ${token}`)
      .send({ settings })
    expect(res.status).toBe(200)
  }

  const readLog = () =>
    fs
      .readFileSync(logPath, 'utf8')
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line))

  const scraperRuns = () => readLog().filter((entry) => entry.script === 'APIscraper_v2.py')
  const inlineRuns = () =>
    scraperRuns().filter((entry) => entry.args[entry.args.indexOf('--extract-mode') + 1] === 'inline')

  const waitFor = async (predicate, what) => {
    const deadline = Date.now() + 10_000
    while (!predicate()) {
      if (Date.now() > deadline) throw new Error(`${what} did not happen; log: ${JSON.stringify(readLog())}`)
      await new Promise((resolve) => setTimeout(resolve, 25))
    }
  }

  // Waits for the extraction's "done" event and then for the shared-upload
  // cleanup child, the last one the route spawns, so no child outlives the test.
  const extract = async (baseName) => {
    const filename = `${baseName}.pdf`
    fs.writeFileSync(path.join(uploadsDir, filename), '%PDF-1.4\n%%EOF\n')
    const res = await request(app)
      .post(`/api/extract-bibliography/${encodeURIComponent(filename)}`)
      .set('Authorization', `Bearer ${token}`)
      .send({})
    expect(res.status).toBe(202)
    await waitFor(
      () => events.some((e) => e.kind === 'extraction' && e.event === 'done' && e.base_name === baseName),
      `extraction of ${baseName}`
    )
    await waitFor(
      () => readLog().some((entry) => entry.script === 'llm_delete_upload.py' && entry.finished),
      `upload cleanup after ${baseName}`
    )
  }

  test('the inline fallback uses the Admin inline model', async () => {
    await putSettings({ inline_model: 'adm-pro-x', inline_cheap_model: 'adm-cheap-x' })
    process.env.FAKE_GET_BIB_EXIT = '1'
    await extract('fallback-admin')
    const runs = scraperRuns()
    expect(runs).toHaveLength(1)
    expect(runs[0].inline).toBe('adm-pro-x')
    expect(runs[0].args).toEqual(expect.arrayContaining(['--extract-mode', 'inline']))
  }, 15_000)

  test('the zero-yield escalation uses the Admin cheap model, then the Admin fallback model', async () => {
    await putSettings({ inline_model: 'adm-pro-x', inline_cheap_model: 'adm-cheap-x' })
    process.env.FAKE_GET_BIB_EXIT = '0'
    await extract('escalation-admin')
    const pageRuns = scraperRuns().filter((entry) => entry.args.includes('page'))
    expect(pageRuns).toHaveLength(1)
    expect(inlineRuns().map((entry) => entry.inline)).toEqual(['adm-cheap-x', 'adm-pro-x'])
  }, 15_000)

  test('without Admin settings the env, then the defaults, apply', async () => {
    await putSettings({ inline_model: '', inline_cheap_model: '' })
    process.env.FAKE_GET_BIB_EXIT = '1'
    process.env.RAG_FEEDER_API_INLINE_MODEL = 'env-pro'
    try {
      await extract('fallback-env')
      expect(scraperRuns().map((entry) => entry.inline)).toEqual(['env-pro'])
    } finally {
      delete process.env.RAG_FEEDER_API_INLINE_MODEL
    }

    fs.writeFileSync(logPath, '')
    events.length = 0
    process.env.FAKE_GET_BIB_EXIT = '0'
    await extract('escalation-defaults')
    expect(inlineRuns().map((entry) => entry.inline)).toEqual(['gemini-3-flash-preview', 'gemini-3.1-pro-preview'])
  }, 25_000)
})
