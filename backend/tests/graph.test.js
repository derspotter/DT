import request from 'supertest'
import { createApp } from '../src/app.js'

describe('GET /api/graph (stub)', () => {
  let app

  beforeAll(() => {
    process.env.RAG_FEEDER_STUB = '1'
    app = createApp({ broadcast: () => {} })
  })

  afterAll(() => {
    delete process.env.RAG_FEEDER_STUB
  })

  test('returns 3D graph payload', async () => {
    const res = await request(app).get('/api/graph/3d?max_nodes=1000&relationship=both')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('nodes')
    expect(res.body).toHaveProperty('edges')
    expect(res.body).toHaveProperty('stats')
    expect(res.body).toHaveProperty('source', 'stub')
  })

  test('returns 3D graph snapshot manifest', async () => {
    const res = await request(app).get('/api/graph/3d/snapshot?max_nodes=1000&relationship=both')
    expect(res.status).toBe(200)
    expect(res.body).toHaveProperty('source', 'stub')
    expect(res.body).toHaveProperty('snapshot_key')
    expect(res.body).toHaveProperty('schema_version', 2)
    expect(res.body).toHaveProperty('layout.algorithm')
    expect(res.body).toHaveProperty('buffers.nodes.count')
    expect(res.body).toHaveProperty('nodes_meta')
  })
})
