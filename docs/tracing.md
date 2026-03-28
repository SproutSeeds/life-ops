# Tracing

`life-ops` now captures structured behavior traces so you can study how the system made agenda and communication decisions and later turn those traces into training data.

## What is traced

- `gmail_sync`: every scanned Gmail message, its triage score, classification result, body preview, attachment metadata, and the final threads selected as `open` or `reference`
- `gmail_backfill`: older Gmail history crawls, including the effective archive query, the resume cursor, and any threads skipped because a newer thread snapshot already existed locally
- `gmail_backfill_exhaustive`: the resumable backfill loop that keeps running archive pulls until Gmail reports no older matching mail or the loop is intentionally interrupted
- `gmail_category_pass`: focused life-admin and archive sweeps that search category-shaped slices of Gmail history before a broader corpus crawl
- `gmail_corpus_sync`: the top-to-bottom orchestration run that combines recent sync, archive backfill loops, and local reclassification
- `gmail_reclassify`: local taxonomy pushes that relabel already-stored Gmail rows after classifier improvements
- `google_calendar_sync`: calendar sync windows, alias cleanup, and synced events
- `agenda_render`: the items that were included in each agenda view

## Why this matters

These traces make it possible to build higher-quality datasets later:

- supervised examples of keep vs filter decisions
- supervised examples of `open` vs `reference` vs `ignore`
- examples of category labels like `billing`, `career`, `community`, `creative`, `developer`, `finance`, `identity`, `logistics`, `medical`, `shopping`, `tax`, and `travel`
- examples of what the agenda chose to surface
- auditability when the system feels wrong

## Commands

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops trace-summary
zsh ./bin/life-ops trace-summary --trace-type gmail_sync --format json
zsh ./bin/life-ops trace-summary --trace-type gmail_backfill --format json
zsh ./bin/life-ops trace-summary --trace-type gmail_backfill_exhaustive --format json
zsh ./bin/life-ops trace-summary --trace-type gmail_category_pass --format json
zsh ./bin/life-ops trace-summary --trace-type gmail_corpus_sync --format json
zsh ./bin/life-ops trace-summary --trace-type gmail_reclassify --format json
zsh ./bin/life-ops export-traces --trace-type gmail_sync --limit 500 --output data/exports/gmail_sync.jsonl
zsh ./bin/life-ops export-traces --trace-type agenda_render --format json
```

## Export shape

Exports include:

- run metadata
- run summaries
- event type
- entity key
- event payload
- timestamps

## Notes

- traces are local-first and live in the same SQLite database as the rest of the system
- Gmail trace payloads can contain message subjects, senders, snippets, body previews, and attachment filenames, so treat exports as sensitive personal data
- if you want a safer downstream pipeline later, add a redaction step before training or sharing exports
