import Database from 'better-sqlite3'
import { tableExists, tableHasColumn, listTableColumns, parseJson } from '../src/dbHelpers.js'

let db
beforeEach(() => {
  db = new Database(':memory:')
})
afterEach(() => {
  db.close()
})

test('tableExists follows the schema', () => {
  expect(tableExists(db, 't')).toBe(false)
  db.exec('CREATE TABLE t (a)')
  expect(tableExists(db, 't')).toBe(true)
  db.exec('DROP TABLE t')
  expect(tableExists(db, 't')).toBe(false)
})

test('listTableColumns of a missing table is empty', () => {
  expect(listTableColumns(db, 't')).toEqual([])
  db.exec('CREATE TABLE t (a)')
  expect(listTableColumns(db, 't')).toEqual(['a'])
})

test('tableHasColumn sees a column added later', () => {
  db.exec('CREATE TABLE t (a)')
  expect(tableHasColumn(db, 't', 'b')).toBe(false)
  db.exec('ALTER TABLE t ADD COLUMN b')
  expect(tableHasColumn(db, 't', 'b')).toBe(true)
})

test('parseJson keeps seed.js rule', () => {
  const fb = { fallback: true }
  expect(parseJson(null, fb)).toBe(fb)
  expect(parseJson(undefined, fb)).toBe(fb)
  expect(parseJson('', fb)).toBe(fb)
  expect(parseJson('{"a":1}', fb)).toEqual({ a: 1 })
  expect(parseJson('null', fb)).toBeNull()
  expect(parseJson('{bad', fb)).toBe(fb)
  const obj = { x: 1 }
  expect(parseJson(obj, fb)).toBe(obj)
  expect(parseJson('{bad')).toBeNull()
})
