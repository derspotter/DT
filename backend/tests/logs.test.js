import fs from 'fs'
import os from 'os'
import path from 'path'
import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/logs/tail', () => {
  let tmpDir = ''
  let app

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'rag-feeder-logs-'))
    process.env.RAG_FEEDER_LOG_DIR = tmpDir

    fs.writeFileSync(
      path.join(tmpDir, 'backend-pipeline.log.1'),
      ['[older] line-a', '[older] line-b', '[older] line-c'].join('\n') + '\n',
      'utf8'
    )
    fs.writeFileSync(
      path.join(tmpDir, 'backend-pipeline.log'),
      ['[new] line-d', '[new] line-e', '[new] line-f'].join('\n') + '\n',
      'utf8'
    )
    fs.writeFileSync(path.join(tmpDir, 'backend-app.log'), ['[app] one', '[app] two'].join('\n') + '\n', 'utf8')

    app = createApp({ broadcast: () => {} })
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
    delete process.env.RAG_FEEDER_LOG_DIR
    if (tmpDir) {
      fs.rmSync(tmpDir, { recursive: true, force: true })
    }
  })

  test('returns tail from pipeline log (including rotated backups)', async () => {
    const res = await request(app).get('/api/logs/tail?type=pipeline&lines=4')
    expect(res.status).toBe(200)
    expect(res.body.type).toBe('pipeline')
    expect(res.body.entries).toEqual(['[older] line-c', '[new] line-d', '[new] line-e', '[new] line-f'])
    expect(res.body.count).toBe(4)
    expect(res.body.has_more).toBe(true)
    expect(res.body.next_cursor).toBe(4)
    expect(res.body.source_exists).toBe(true)
  })

  test('supports cursor-based pagination for older log lines', async () => {
    const newer = await request(app).get('/api/logs/tail?type=pipeline&lines=4')
    expect(newer.status).toBe(200)
    const older = await request(app).get(`/api/logs/tail?type=pipeline&lines=4&cursor=${newer.body.next_cursor}`)
    expect(older.status).toBe(200)
    expect(older.body.entries).toEqual(['[older] line-a', '[older] line-b'])
    expect(older.body.has_more).toBe(false)
    expect(older.body.next_cursor).toBe(6)
  })

  test('returns app log tail', async () => {
    const res = await request(app).get('/api/logs/tail?type=app&lines=2')
    expect(res.status).toBe(200)
    expect(res.body.type).toBe('app')
    expect(res.body.entries).toEqual(['[app] one', '[app] two'])
    expect(res.body.count).toBe(2)
  })

  test('rejects invalid log type', async () => {
    const res = await request(app).get('/api/logs/tail?type=invalid')
    expect(res.status).toBe(400)
    expect(String(res.body.error || '')).toContain("Invalid log type")
  })
})
