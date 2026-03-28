# Profile Context

`life-ops` can build a separate candidate layer of high-value personal records from the stored Gmail corpus, then promote approved candidates into a canonical profile memory layer.

This is meant for things like:

- identity documents
- insurance records
- immigration records
- tax records
- medical records
- benefits records

It is stricter than normal Gmail classification. The extractor favors:

- subject lines
- Gmail snippets
- attachment filenames
- trusted domains

That keeps the profile layer more document-shaped and less likely to absorb random newsletters or promotional mail.

## Build the profile layer

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops ingest-profile-attachments --limit 25
zsh ./bin/life-ops ingest-profile-attachments --scope sensitive --limit 100
zsh ./bin/life-ops backfill-profile-attachments --max-results 100 --max-runs 0
zsh ./bin/life-ops profile-attachment-heartbeat
zsh ./bin/life-ops extract-profile-context
```

`ingest-profile-attachments` downloads Gmail attachments into the local attachment vault at `data/attachments/`.

- `--scope profile` targets the current candidate profile records.
- `--scope sensitive` expands the sweep to Gmail already classified into sensitive life buckets like identity, insurance, immigration, tax, medical, benefits, and record keeping.

The ingest step extracts text from:

- PDFs with `pdftotext`
- images with `tesseract`
- text and HTML files directly

It also skips common decorative/template attachments like logos, bars, corners, and tiny inline assets so the vault stays document-heavy.

That extracted text is folded back into the next `extract-profile-context` run.

If you want the sensitive-document sweep to keep walking the full stored Gmail corpus, use `backfill-profile-attachments`. It keeps a local cursor in SQLite and can run until exhaustion with `--max-runs 0`.

## Inspect the extracted candidates

```bash
zsh ./bin/life-ops profile-context-summary
zsh ./bin/life-ops profile-context --item-type identity_document
zsh ./bin/life-ops profile-context --item-type insurance_record
zsh ./bin/life-ops profile-context --item-type immigration_record
zsh ./bin/life-ops profile-context --subject-key wife_sisy
zsh ./bin/life-ops profile-review-next
zsh ./bin/life-ops profile-approve --id 123 --notes "confirmed from PDF"
zsh ./bin/life-ops profile-reject --id 124 --notes "newsletter false positive"
zsh ./bin/life-ops profile-records
zsh ./bin/life-ops profile-record-show --id 1
zsh ./bin/life-ops profile-record-summary
zsh ./bin/life-ops profile-alerts
zsh ./bin/life-ops attachment-summary
zsh ./bin/life-ops attachments --limit 10
zsh ./bin/life-ops profile-review-set --id 123 --status approved --notes "confirmed from PDF"
```

## Canonical profile memory

`profile-approve` promotes a candidate into the canonical profile layer. That layer now keeps:

- profile subjects like `self` and `wife_sisy`
- canonical records for documents, policies, cases, and records
- linked source candidates
- linked extracted attachments

Use `profile-merge` when two candidates are really the same real-world record and should point at one canonical record:

```bash
zsh ./bin/life-ops profile-merge --id 125 --record-id 1 --notes "same insurance policy, newer attachment"
```

Use `profile-alerts` to surface the records that should stay especially visible, like immigration movement, benefits renewals, tax packets, or important records that still do not have a linked extracted attachment.

## Notes

- Items are stored locally in the same SQLite database as the rest of `life-ops`.
- Downloaded attachment files are stored locally under `data/attachments/`.
- Candidate extraction now preserves approved and rejected review decisions on re-extract instead of wiping them out.
- Canonical profile records are also stored locally in the same SQLite database.
- This pass now extracts text from many PDFs and images, but it still is not a full OCR/document-intelligence system.
- It works best after `reclassify-gmail` so the stored corpus reflects the newest taxonomy rules.
- `profile-attachment-heartbeat` gives a live checkpoint on where the resumable sensitive-attachment crawl is in the local Gmail archive.
