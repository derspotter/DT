# User Guide

## Purpose

The Korpus Builder is a web application for building literature corpora from two starting points:

- a keyword search
- a seed document such as a paper or book chapter

The application helps you collect references, enrich them with metadata, download available PDFs, review the resulting corpus, inspect citation structure, and export the final result.

Everything you do is scoped to the currently selected corpus.

## Access

1. Open the application in your browser.
2. Log in with your username and password.
3. If you were invited by email, use the invite link first to set your password.
4. If you forgot your password, use the `Forgot password?` link on the login screen.

## Main Navigation

The application is organized into these top-level areas:

- `Workspace`
- `Downloads`
- `Graph`
- `Pipeline Status`
- `Logs`

The most important daily work happens in `Workspace`.

## Select or Create a Corpus

The corpus selector is in the top-right header.

- Use the `Corpus` dropdown to switch between corpora you can access.
- Click `New corpus` to create a new empty working corpus.
- If you have permission, you can also delete the currently selected corpus.

Important:

- all search, seed review, promotion, enrichment, download state, graph view, and exports are tied to the selected corpus
- changing the selected corpus changes the whole working context

## Workspace Overview

The workspace is structured into three numbered steps:

1. `Search`
2. `Seed`
3. `Corpus`

This reflects the typical workflow:

1. find candidate references
2. review and promote candidates
3. inspect the resulting corpus

## 1. Search

Use `Search` when you want to find literature from a query instead of starting from an uploaded paper.

You can:

- enter free-text search queries
- use Boolean operators such as `AND`, `OR`, and `NOT`
- adjust search options such as recursion depth and result limits

Typical flow:

1. Enter a query.
2. Run the search.
3. Review the returned references in the Seed area.
4. Promote relevant references into the current corpus.

The search results are persisted in the database and remain tied to the current corpus as a seed source.

## Upload a Seed Document

Use the upload area in `Search` when you want to start from a paper, book chapter, or other document.

Supported seed inputs include:

- PDF
- BibTeX
- JSON

Typical PDF flow:

1. Upload a PDF.
2. Start bibliography extraction.
3. Wait for extraction to finish.
4. Review extracted candidates in the Seed area.
5. Promote relevant candidates into the corpus.

If a bibliography section is not found, the system may fall back to inline extraction.

## 2. Seed

The Seed pane shows candidate references collected from:

- uploaded seed documents
- keyword searches

Each source can be expanded to inspect its candidates.

For each candidate you can see:

- title
- current pipeline stage
- whether it is already in the current corpus
- whether it is downloaded elsewhere

Important behavior:

- candidates already in the current corpus are not selectable again
- clicking a row shows inline details
- the checkbox controls promotion selection
- the source-level arrow promotes all currently promotable candidates from that source

Typical review flow:

1. Expand a source.
2. Select relevant candidates with checkboxes, or promote the whole source.
3. Click `Promote to Corpus`.

Promotion adds the selected works to the current corpus immediately. It does not wait for enrichment or download to finish.

## 3. Corpus

The Corpus pane shows the references that are already part of the selected corpus.

Items may be in different stages, for example:

- `raw`
- `enriching`
- `matched`
- `queued_download`
- `downloaded`
- `failed_enrichment`
- `failed_download`

These stages describe the processing state of each work. They do not affect whether the work belongs to the corpus.

Use the Corpus pane to:

- inspect which works are already in the corpus
- filter by processing stage
- open inline details for a row
- check whether enrichment or download failed for a specific work

## Downloads

The `Downloads` tab shows download activity for the current corpus.

It includes:

- queued downloads
- active downloads
- recent completed downloads

This view is useful for checking whether the downloader is progressing and which PDFs were successfully retrieved.

## Graph

The `Graph` tab provides a citation-network view of the current corpus.

You can:

- switch relationship mode
- filter by status
- limit year ranges
- limit node count
- choose color grouping

Use it to inspect structure, clusters, and connectivity in the corpus.

## Pipeline Status

The `Pipeline Status` tab shows processing state for the selected corpus.

It includes:

- current corpus counts
- current corpus backlog
- recent downloads

Admin users also see a separate global overview across the whole application.

## Logs

The `Logs` tab gives access to:

- pipeline logs
- app logs

Use it when:

- extraction seems stuck
- a search or promotion did not behave as expected
- enrichment or download failures need to be traced

## Export

The application can export the current corpus in several formats:

- JSON
- BibTeX
- PDFs as ZIP
- bundle ZIP

Exports operate on the currently selected corpus.

## Kantropos Target Assignment

In the header, next to the corpus selector, you can assign the current local corpus to a predefined Kantropos target corpus.

This assignment is:

- corpus-specific
- persisted in the database
- intended as preparation for later export into the fixed Kantropos corpus structure

Important:

- the target assignment does not itself export files yet
- it records which Kantropos corpus this local corpus is meant to feed

## Password Reset

If you forget your password:

1. Open the login page.
2. Click `Forgot password?`
3. Enter your email address.
4. Open the reset link from your mailbox.
5. Set a new password.

## Practical Recommended Workflow

For most users, the shortest useful workflow is:

1. Select or create a corpus.
2. Run a keyword search or upload a seed PDF.
3. Review the resulting Seed candidates.
4. Promote relevant items into the corpus.
5. Watch processing in `Corpus`, `Downloads`, or `Pipeline Status`.
6. Inspect the citation network in `Graph`.
7. Export the final corpus.
