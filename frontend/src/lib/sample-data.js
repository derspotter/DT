export const sampleSearchResults = [
  {
    id: 'W2175056322',
    title: 'The New Institutional Economics',
    authors: 'Ronald H. Coase',
    year: 2002,
    type: 'book-chapter',
    doi: '10.1017/cbo9780511613807.002',
  },
  {
    id: 'W2015930340',
    title: 'The Nature of the Firm',
    authors: 'Ronald H. Coase',
    year: 1937,
    type: 'journal-article',
    doi: '10.1111/j.1468-0335.1937.tb00002.x',
  },
  {
    id: 'W1974636593',
    title: 'Markets and Hierarchies',
    authors: 'Oliver E. Williamson',
    year: 1975,
    type: 'book',
    doi: '',
  },
]

export const sampleCorpus = [
  {
    id: 'W2175056322',
    title: 'The New Institutional Economics',
    year: 2002,
    source: 'HAndbook of New Institutional Economics 2025',
    status: 'matched',
  },
  {
    id: 'W2015930340',
    title: 'The Nature of the Firm',
    year: 1937,
    source: 'Seed bibliography',
    status: 'downloaded',
  },
  {
    id: 'W1974636593',
    title: 'Markets and Hierarchies',
    year: 1975,
    source: 'Keyword search',
    status: 'queued_download',
  },
]

export const sampleDownloads = [
  {
    id: 'DL-2025-001',
    title: 'The New Institutional Economics',
    status: 'queued',
    attempts: 0,
  },
  {
    id: 'DL-2025-002',
    title: 'Markets and Hierarchies',
    status: 'in_progress',
    attempts: 1,
  },
  {
    id: 'DL-2025-003',
    title: 'The Nature of the Firm',
    status: 'completed',
    attempts: 1,
  },
]

export const sampleActivity = [
  {
    id: 'RUN-001',
    label: 'Seed PDF ingestion',
    detail: '4 references extracted',
    timestamp: '2026-01-31 09:14',
  },
  {
    id: 'RUN-002',
    label: 'Keyword search',
    detail: '12 results queued',
    timestamp: '2026-01-31 10:02',
  },
  {
    id: 'RUN-003',
    label: 'Download queue',
    detail: '3 PDFs completed',
    timestamp: '2026-01-31 11:20',
  },
]
