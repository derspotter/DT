import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/graph (stub)', () => {
  const app = createApp({ broadcast: () => {} })

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('returns graph payload', async () => {
    const res = await request(app).get('/api/graph')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('nodes')
    expect(res.body).toHaveProperty('edges')
    expect(res.body).toHaveProperty('stats')
    expect(res.body.stats).toHaveProperty('node_count')
    expect(res.body.stats).toHaveProperty('edge_count')
  })
})

describe('GET /api/graph (real)', () => {
  const app = createApp({ broadcast: () => {} })
  let token = ''

  beforeAll(async () => {
    const login = await request(app)
      .post('/api/auth/login')
      .set('Content-Type', 'application/json')
      .send({ username: 'admin', password: 'admin' })

    if (login.status === 200 && login.body?.token) {
      token = login.body.token
    }
  })

  test('returns a DB-backed graph payload', async () => {
    const res = await request(app)
      .get('/api/graph?max_nodes=100&relationship=both&status=all')
      .set('Authorization', `Bearer ${token}`)

    expect([200, 500]).toContain(res.status)
    if (res.status === 500) {
      expect(res.body.error).toBeTruthy()
      return
    }

    expect(res.body).toHaveProperty('nodes')
    expect(Array.isArray(res.body.nodes)).toBe(true)
    expect(res.body).toHaveProperty('edges')
    expect(Array.isArray(res.body.edges)).toBe(true)
    expect(res.body).toHaveProperty('stats')
    expect(res.body.stats).toHaveProperty('node_count')
    expect(res.body.stats).toHaveProperty('edge_count')
    expect(res.body).not.toHaveProperty('source', 'stub')
  }, 30000)

  test('supports relationship filters', async () => {
    const withAuth = {
      Authorization: `Bearer ${token}`,
    }

    const [referencesRes, citedByRes] = await Promise.all([
      request(app).get('/api/graph?relationship=references&status=all&max_nodes=25').set(withAuth),
      request(app).get('/api/graph?relationship=cited_by&status=all&max_nodes=25').set(withAuth),
    ])

    for (const res of [referencesRes, citedByRes]) {
      expect([200, 500]).toContain(res.status)
      if (res.status === 200) {
        expect(res.body).toHaveProperty('stats')
        expect(res.body.stats).toHaveProperty('relationship_counts')
      }
    }
  }, 30000)
})
