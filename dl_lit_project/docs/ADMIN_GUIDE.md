# Admin Guide

## Purpose

This guide covers the tasks that are specific to administrators and operators of the Korpus Builder.

It focuses on:

- user and access management
- corpus administration
- Kantropos target assignment
- mail configuration for invites and password resets
- operational safety

## Core Responsibilities

An admin is expected to handle these tasks:

- create and invite users
- manage shared access to corpora
- create or remove corpora when needed
- assign local corpora to predefined Kantropos target corpora
- monitor system health and worker progress
- back up the database before risky changes

## Accounts and Invitations

Users are created by an admin and onboarded through an email invitation.

Current flow:

1. Open the app as an admin.
2. Use the `Invite user` control in the header.
3. Enter:
   - username
   - email address
4. Send the invitation.
5. The user receives a link to set their password.

This is the preferred onboarding flow because:

- the admin stays in control of account creation
- the user sets their own password
- password reset can happen without admin intervention

## Password Resets

Users can reset their own password through the login page if SMTP is configured.

Admin action is only needed when:

- the email address is wrong
- mail delivery is broken
- the account itself needs correction

## Admin Bootstrap Account

The system may start with a bootstrap admin account from environment variables.

Important:

- do not keep default bootstrap credentials in long-term use
- set a real email address for the admin account
- use the password reset flow to establish a secure password if needed

## Corpus Management

Admins can:

- create corpora
- delete corpora
- access shared corpora

Each user normally starts with a default corpus named from the owner, for example:

- `admin's Default Corpus`
- `testuser2's Default Corpus`

This avoids ambiguity when multiple users have shared default corpora visible at once.

## Shared Access to Corpora

Corpora can be shared with other users.

Current roles:

- `owner`
- `editor`
- `viewer`

Use sharing when:

- multiple users collaborate on the same literature corpus
- one person curates and another only reviews
- an admin needs visibility into a user corpus

Use separate corpora when:

- the workstreams are unrelated
- ownership boundaries matter
- review noise from shared seed sources would be confusing

## Kantropos Target Assignment

Each local corpus can be assigned to one predefined Kantropos target corpus.

The selector appears in the header next to the local corpus selector.

Current meaning of this assignment:

- it records which fixed Kantropos corpus the local corpus belongs to
- it does not yet perform the actual export by itself

The predefined targets are configured on the backend and currently resolve under:

- `/data/projects/kantropos/corpora`

Example target:

- `Anthropozän & Nachhaltiges Management`

Use this assignment consistently so the later export step is unambiguous.

## Mail Configuration

Invites and password reset depend on SMTP.

Relevant variables are stored in the repository root `.env`:

- `RAG_FEEDER_FRONTEND_URL`
- `RAG_FEEDER_SMTP_HOST`
- `RAG_FEEDER_SMTP_PORT`
- `RAG_FEEDER_SMTP_SECURE`
- `RAG_FEEDER_SMTP_USER`
- `RAG_FEEDER_SMTP_PASS`
- `RAG_FEEDER_SMTP_FROM`
- `RAG_FEEDER_SMTP_TLS_REJECT_UNAUTHORIZED`
- `RAG_FEEDER_SMTP_IGNORE_TLS`

Current working pattern:

- Gmail SMTP with an app password

Operational rule:

- use an app password, not the mailbox login password

After changing SMTP settings:

1. update `.env`
2. restart the backend service

## What to Back Up

The primary data asset is the SQLite database:

- `dl_lit_project/data/literature.db`

Back this up before:

- schema changes
- bulk imports
- destructive corpus cleanup
- any one-off repair scripts

Recommended quick backup:

```bash
cp dl_lit_project/data/literature.db dl_lit_project/data/literature.db.bak.$(date +%F-%H%M%S)
```

## What Deleting a Corpus Does

Deleting a corpus removes corpus-scoped state such as:

- corpus membership
- shares
- ingest rows tied to that corpus
- queued jobs tied to that corpus

It does not delete the global canonical work catalog. The application now keeps collected works globally and models corpus membership separately.

## Operational Checks

As an admin, use these UI areas regularly:

- `Pipeline Status`
  - current corpus counts
  - current corpus backlog
  - global overview
- `Downloads`
  - recent downloaded items for the selected corpus
- `Logs`
  - pipeline and app logs

These are the quickest ways to detect:

- stalled enrichment
- empty queues
- repeated failures
- extraction issues

## Current Boundaries

These are important to understand operationally:

- Kantropos target assignment exists, but automated export to Kantropos is not yet the documented live step
- the current stack uses SQLite, not PostgreSQL
- the app keeps works globally and maps them into corpora through corpus membership

## Recommended Admin Workflow

1. Create or select the target corpus.
2. Assign its Kantropos target if relevant.
3. Invite the user.
4. Share the corpus if collaboration is needed.
5. Let the user work in `Workspace`.
6. Monitor `Pipeline Status`, `Downloads`, and `Logs`.
7. Export the finished corpus when needed.
