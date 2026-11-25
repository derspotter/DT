import express from 'express'
import request from 'supertest'
import { jest } from '@jest/globals'
import { createFinalErrorHandler } from '../src/app.js'

// BE-59 final review: the last error handler never echoes a filesystem
// error's message (it holds absolute paths), names a non-exposed 4xx by its
// status text, logs every 4xx and labels a 5xx with method and url.
describe('final error handler', () => {
  let warnSpy
  let sendServerError

  const appFailingWith = (error) => {
    const app = express()
    app.get('/fail', () => { throw error })
    app.use(createFinalErrorHandler({ sendServerError }))
    return app
  }

  beforeEach(() => {
    warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {})
    sendServerError = jest.fn((req, res, route, error, fallback) => res.status(500).json({ error: fallback }))
  })

  afterEach(() => {
    warnSpy.mockRestore()
  })

  test('an exposed filesystem 404 answers the status text, not the path', async () => {
    const error = Object.assign(new Error("ENOENT: no such file or directory, stat '/abs/secret'"), {
      status: 404, expose: true, syscall: 'stat', code: 'ENOENT', path: '/abs/secret',
    })
    const res = await request(appFailingWith(error)).get('/fail')
    expect(res.status).toBe(404)
    expect(res.body).toEqual({ error: 'Not Found' })
    expect(res.text).not.toContain('/abs/secret')
    expect(warnSpy).toHaveBeenCalledWith('[express] GET /fail 404:', error)
  })

  test('a non-exposed 413 answers its status text', async () => {
    const error = Object.assign(new Error('request entity too large: 99999 bytes'), { status: 413, expose: false })
    const res = await request(appFailingWith(error)).get('/fail')
    expect(res.status).toBe(413)
    expect(res.body).toEqual({ error: 'Payload Too Large' })
    expect(warnSpy).toHaveBeenCalledTimes(1)
  })

  test('an exposed body-parser 400 keeps its message', async () => {
    const error = Object.assign(new Error('Unexpected end of JSON input'), {
      status: 400, statusCode: 400, expose: true, type: 'entity.parse.failed',
    })
    const res = await request(appFailingWith(error)).get('/fail')
    expect(res.status).toBe(400)
    expect(res.body).toEqual({ error: 'Unexpected end of JSON input' })
    expect(warnSpy).toHaveBeenCalledWith('[express] GET /fail 400:', error)
  })

  test('a 5xx goes to sendServerError labelled with method and url', async () => {
    const error = new Error('boom')
    const res = await request(appFailingWith(error)).get('/fail?x=1')
    expect(res.status).toBe(500)
    expect(sendServerError).toHaveBeenCalledTimes(1)
    const [, , route, passed, fallback] = sendServerError.mock.calls[0]
    expect(route).toBe('express GET /fail?x=1')
    expect(passed).toBe(error)
    expect(fallback).toBe('Internal server error')
    expect(warnSpy).not.toHaveBeenCalled()
  })
})
