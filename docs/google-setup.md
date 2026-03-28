# Google Setup

This repo now supports Google Calendar and Gmail sync from the command line.

## 1. Create Google credentials

- open Google Cloud Console
- create or choose a project
- enable `Google Calendar API`
- enable `Gmail API`
- create an OAuth client of type `Desktop app`
- download the credentials JSON
- place it at `config/google_credentials.json`

## 2. Install the repo dependencies

```bash
cd /Volumes/Code_2TB/code/life-ops
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## 3. Authenticate once

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops google-auth
```

That opens the local OAuth flow in your browser and writes the token cache to `data/google_token.json`.

## 4. Inspect your calendars

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops google-list-calendars
```

If you do nothing else, the sync command uses the `primary` calendar.

## 5. Sync everything

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops sync-google
```

## Useful variants

```bash
zsh ./bin/life-ops sync-google-calendar --calendar-id primary --days-ahead 45
zsh ./bin/life-ops sync-gmail --max-results 40
zsh ./bin/life-ops sync-gmail --query 'in:inbox is:unread newer_than:14d'
zsh ./bin/life-ops backfill-gmail --max-results 1000
zsh ./bin/life-ops backfill-gmail --max-results 1000 --reset-cursor
zsh ./bin/life-ops sync-gmail-category-pass --max-results 100 --reset-cursors
zsh ./bin/life-ops sync-gmail-corpus --backfill-query '-in:chats -category:promotions -category:social -category:forums' --backfill-max-results 250 --backfill-max-runs 3
zsh ./bin/life-ops sync-gmail-corpus --backfill-query=-in:chats --backfill-max-results 250 --backfill-max-runs 0 --reset-backfill-cursor
zsh ./bin/life-ops reclassify-gmail
```

## Current behavior

- calendar events sync into the local `events` table
- Gmail sync scans recent unread inbox mail, extracts message bodies plus attachment metadata, and groups repeated notifications by thread
- Gmail threads are classified as `open`, `reference`, or `ignore`
- current built-in categories include `billing`, `benefits`, `career`, `collaboration`, `community`, `creative`, `developer`, `entertainment`, `finance`, `home`, `identity`, `insurance`, `logistics`, `medical`, `pets`, `record_keeping`, `scheduling`, `security`, `shopping`, `tax`, and `travel`
- `open` Gmail threads sync into the local `communications` table as agenda follow-up items
- `reference` Gmail threads are also stored locally and can be inspected with `zsh ./bin/life-ops comms --status reference --source gmail`
- use `reclassify-gmail` when classifier rules improve and you want the stored Gmail corpus to pick up the new labels locally
- `backfill-gmail` pages older history into the same store, starting just before the recent-sync cutoff and then resuming from the saved cursor on later runs
- `sync-gmail-category-pass` runs a focused set of category-oriented history sweeps before a broader archive pull
- `sync-gmail-corpus` chains recent sync, repeated backfill loops, and a local reclassification sweep into one corpus-building command
- pass `--backfill-max-runs 0` to `sync-gmail-corpus` when you want it to keep running until the matching Gmail backlog is exhausted instead of stopping at a fixed loop count
- backfill skips older thread snapshots when a newer copy of that same Gmail thread is already stored locally
- Gmail follow-ups are surfaced in the agenda by giving open items a same-day follow-up time at sync time
- cancelled calendar events are kept in storage but hidden from the agenda view

## Notes

- if you change scopes or replace the Google app credentials, delete `data/google_token.json` and run `google-auth` again
- Google’s latest quickstarts currently call out Python `3.10.7+`; this repo remains Python `3.9` compatible, but if auth tooling gives you trouble, using a `3.10+` virtualenv is the safest path
