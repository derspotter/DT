import { expect, test } from '@playwright/test'
import {
  WORK_STATES,
  workState,
  bucketForStatus,
  metadataLabel,
  downloadLabel,
  parseAuthors,
  formatAuthors,
  formatAuthorsShort,
  normalizeWorkItem,
  cellText,
  nextSort,
  sortIndicator,
  parseSort,
} from '../src/lib/works.js'

test('parseAuthors keeps "Last, First" names intact when they arrive as an array', () => {
  expect(parseAuthors(['Smith, J.', 'Doe, A.'])).toEqual(['Smith, J.', 'Doe, A.'])
  expect(parseAuthors('["Smith, J.", "Doe, A."]')).toEqual(['Smith, J.', 'Doe, A.'])
  expect(parseAuthors('A. One; B. Two')).toEqual(['A. One', 'B. Two'])
  expect(parseAuthors('A. One, B. Two')).toEqual(['A. One', 'B. Two'])
  expect(parseAuthors([' A ', '', null, 'B'])).toEqual(['A', 'B'])
  expect(parseAuthors('')).toEqual([])
  expect(parseAuthors(null)).toEqual([])
})

test('formatAuthors joins the full list; formatAuthorsShort caps at three with et al.', () => {
  expect(formatAuthors({ authors: ['Smith, J.', 'Doe, A.'] })).toBe('Smith, J., Doe, A.')
  expect(formatAuthors({ author: ['X'] })).toBe('X')
  expect(formatAuthors(null)).toBe('')
  expect(formatAuthorsShort({ authors: ['A', 'B', 'C', 'D'] })).toBe('A, B, C et al.')
  expect(formatAuthorsShort({ authors: ['A', 'B', 'C'] })).toBe('A, B, C')
  expect(formatAuthorsShort({ authors: ['A', 'B', 'C'] }, 2)).toBe('A, B et al.')
})

test('normalizeWorkItem never re-splits "Last, First" names (the Corpus bug)', () => {
  const item = normalizeWorkItem({
    id: 1,
    title: 'T',
    authors: '["Smith, J.", "Doe, A.", "Roe, B.", "Poe, E."]',
  })
  expect(item.id).toBe(1)
  expect(item.title).toBe('T')
  expect(item.authors_list).toEqual(['Smith, J.', 'Doe, A.', 'Roe, B.', 'Poe, E.'])
  expect(item.authors_display).toBe('Smith, J., Doe, A., Roe, B., Poe, E.')
  expect(item.authors_short).toBe('Smith, J., Doe, A., Roe, B. et al.')
  expect(cellText(item, 'authors')).toBe('Smith, J., Doe, A., Roe, B. et al.')
  expect(normalizeWorkItem(null)).toBe(null)
})

test('cellText blank default is per caller', () => {
  expect(cellText({}, 'year')).toBe('-')
  expect(cellText({}, 'year', { blank: '' })).toBe('')
  expect(cellText({}, 'doi', { blank: '' })).toBe('')
  expect(cellText({}, 'refs', { blank: '' })).toBe('–')
  expect(cellText({ refs_count: 0 }, 'refs')).toBe(0)
  expect(cellText({}, 'title')).toBe('Untitled')
  expect(cellText({ source: 'S' }, 'title')).toBe('S')
  expect(cellText({ file_path: '/x.pdf' }, 'file')).toBe('Yes')
  expect(cellText({ file_available: true }, 'file')).toBe('Yes')
  expect(cellText({ file_available: false, downloaded_file_path: '/x' }, 'file')).toBe('No')
  expect(cellText({ open_access_url: 'u' }, 'open_access')).toBe('Yes')
  expect(cellText({ added_at: '2026-01-02 03:04:05' }, 'added')).toBe('2026-01-02')
  expect(cellText({ origin: 'search:7' }, 'seed')).toBe('Search #7')
  expect(cellText({}, 'unknown', { blank: '' })).toBe('')
})

test('sort toggles asc → desc → asc on the "column:direction" encoding and the indicator follows', () => {
  expect(nextSort('', 'year')).toBe('year:asc')
  expect(nextSort('year:asc', 'year')).toBe('year:desc')
  expect(nextSort('year:desc', 'year')).toBe('year:asc')
  expect(nextSort('year:desc', 'title')).toBe('title:asc')
  expect(sortIndicator('', 'year')).toBe('')
  expect(sortIndicator('year:asc', 'year')).toBe(' ▲')
  expect(sortIndicator('year:desc', 'year')).toBe(' ▼')
  expect(sortIndicator('year:desc', 'title')).toBe('')
  expect(parseSort('year:desc')).toEqual({ column: 'year', direction: 'desc' })
  expect(parseSort('')).toBe(null)
})

test('workState whitelists the ten states', () => {
  expect(WORK_STATES).toHaveLength(10)
  expect(workState('bogus')).toBe('pending')
  expect(workState(undefined)).toBe('pending')
  expect(workState('DOWNLOADED')).toBe('downloaded')
  expect(workState(' failed_download ')).toBe('failed_download')
})

test('bucketForStatus and labels match the tables', () => {
  expect(bucketForStatus(null)).toBe('raw')
  expect(bucketForStatus('extract_references_from_pdf')).toBe('raw')
  expect(bucketForStatus('matched')).toBe('metadata')
  expect(bucketForStatus('failed_download')).toBe('failed')
  expect(bucketForStatus('downloaded')).toBe('downloaded')
  expect(bucketForStatus('weird')).toBe('raw')
  // Corpus rows: collapsed `status`, no `state`.
  expect(metadataLabel({ status: 'failed_enrichment' })).toBe('Metadata not confirmed')
  expect(metadataLabel({ status: 'matched', metadata_status: 'matched' })).toBe('Confirmed')
  expect(metadataLabel({ status: 'raw', metadata_status: 'in_progress' })).toBe('in progress')
  expect(metadataLabel({ status: 'raw' })).toBe('-')
  expect(downloadLabel({ status: 'failed_download' })).toBe('Not retrievable')
  expect(downloadLabel({ status: 'raw' })).toBe('-')
  // Seed candidates: resolved `state`, falling back when the axis is absent.
  expect(metadataLabel({ state: 'failed_enrichment', metadata_status: 'matched' })).toBe('Metadata not confirmed')
  expect(metadataLabel({ state: 'queued_enrichment' })).toBe('Enriching')
  expect(metadataLabel({ state: 'enriched' })).toBe('Confirmed')
  expect(downloadLabel({ state: 'failed_download' })).toBe('Not retrievable')
  expect(downloadLabel({ state: 'downloaded', file_available: false })).toBe('File missing')
  expect(downloadLabel({ state: 'downloaded_elsewhere', file_available: true })).toBe('Downloaded')
  expect(downloadLabel({ state: 'downloaded_elsewhere' })).toBe('Stale record')
  expect(downloadLabel({ state: 'queued_download' })).toBe('Queued')
  expect(downloadLabel({ state: 'enriched', download_status: 'in_progress' })).toBe('Downloading')
  expect(downloadLabel({ state: 'enriched' })).toBe('-')
})
