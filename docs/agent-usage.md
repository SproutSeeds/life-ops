# Agent Usage

When you ask your agent about the agenda, the agent should query this repo directly rather than guessing from memory.

## Refresh before answering

If Google sync is configured, refresh the store first:

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops sync-google
```

## Human-readable week view

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops agenda --days 7
```

## Machine-readable week view

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops agenda --days 7 --format json
```

## Inspect categorized communications

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops comms --status open
zsh ./bin/life-ops comms --status reference --source gmail
zsh ./bin/life-ops comms --status reference --source gmail --category finance
zsh ./bin/life-ops comms-summary --source gmail
zsh ./bin/life-ops gmail-heartbeat
zsh ./bin/gmail-exhaust status
zsh ./bin/life-ops backfill-gmail --max-results 1000
zsh ./bin/life-ops sync-gmail-category-pass --max-results 100 --reset-cursors
zsh ./bin/life-ops sync-gmail-corpus --backfill-query '-in:chats -category:promotions -category:social -category:forums' --backfill-max-results 250 --backfill-max-runs 3
zsh ./bin/life-ops sync-gmail-corpus --backfill-query=-in:chats --backfill-max-results 250 --backfill-max-runs 0 --reset-backfill-cursor
zsh ./bin/life-ops reclassify-gmail
zsh ./bin/life-ops extract-profile-context
zsh ./bin/life-ops ingest-profile-attachments --limit 25
zsh ./bin/life-ops ingest-profile-attachments --scope sensitive --limit 100
zsh ./bin/life-ops backfill-profile-attachments --max-results 100 --max-runs 0
zsh ./bin/life-ops profile-attachment-heartbeat
zsh ./bin/life-ops attachment-summary
zsh ./bin/life-ops attachments --limit 10
zsh ./bin/life-ops profile-context-summary
zsh ./bin/life-ops profile-context --item-type identity_document
zsh ./bin/life-ops profile-context --item-type immigration_record
zsh ./bin/life-ops profile-context --subject-key wife_sisy
zsh ./bin/life-ops profile-review-set --id 123 --status approved --notes "confirmed from PDF"
zsh ./bin/life-ops keys-list
zsh ./bin/life-ops x-status
zsh ./bin/life-ops x-auth
zsh ./bin/life-ops x-me
zsh ./bin/life-ops x-posts --limit 5
zsh ./bin/life-ops x-user --username XDevelopers
zsh ./bin/life-ops x-home --limit 10
zsh ./bin/life-ops x-package-create --title "Define Your Canonical Dossier" --point "Capture the truth layer first"
zsh ./bin/life-ops x-content
zsh ./bin/life-ops x-content-show --id 1
zsh ./bin/life-ops x-media --content-id 1
```

## Recommended agent behavior

- use the text view when answering "what's on the agenda?"
- use the JSON view when planning, summarizing, or deciding what to move
- treat open communication follow-ups as first-class agenda items
- use `comms --status reference --source gmail` when the user is asking about stored records, benefits notices, tax docs, medical mail, or creative/archive items
- use `comms --category ...` when the user wants a specific slice such as `career`, `community`, `creative`, `developer`, `entertainment`, `finance`, `home`, `logistics`, `medical`, `pets`, `shopping`, `tax`, or `travel`
- use `backfill-gmail` when the user wants older inbox history classified; it resumes from the stored cursor unless `--reset-cursor` is passed
- use `sync-gmail-corpus` when the user wants the fastest top-to-bottom taxonomy push across both recent and archival Gmail in one pass
- use `sync-gmail-category-pass` when the user wants a focused life-admin sweep before a broader archive crawl
- use `sync-gmail-corpus --backfill-max-runs 0` when the user wants the backlog crawl to continue until the matching Gmail history is exhausted
- use `gmail-heartbeat` when the user wants a live progress checkpoint while the backlog runner is still chewing
- use `bin/gmail-exhaust` when you want a simple tmux wrapper for start, restart, status, tail, attach, and stop
- use `reclassify-gmail` after classifier improvements so stored Gmail rows pick up the new taxonomy without forcing another API crawl
- use `extract-profile-context` after inbox sync when the user wants structured records for IDs, insurance, immigration, tax, medical, or benefits context
- use `ingest-profile-attachments` before `extract-profile-context` when the user wants the system to learn from the contents of PDFs and images, not just subjects and snippets
- use `ingest-profile-attachments --scope sensitive` when the user wants a broader pass across already-classified life-admin mail instead of only the current profile candidate slice
- use `backfill-profile-attachments --max-runs 0` when the user wants the sensitive attachment/document walk to continue through the whole stored Gmail corpus until exhausted
- use `profile-attachment-heartbeat` when the user wants a live checkpoint on the sensitive attachment backfill runner
- use `attachment-summary` and `attachments` when you want to inspect the local evidence vault behind profile-context candidates
- use `profile-context-summary` first when the user asks what profile-grade records exist
- use `profile-context --item-type ...` when the user asks for a narrow slice like identity, insurance, or immigration
- use `profile-context --subject-key wife_sisy` when the user asks for spouse-specific context that was inferred from the mailbox
- use `profile-review-set` to approve or reject candidates after a human review pass
- use `keys-list` when you need to verify whether required global secrets are registered for the repo
- use `x-status` when the user asks about the X integration roadmap or current setup readiness
- use `x-auth` when the user is ready to connect the local CLI to their X account
- use `x-me` to verify which X account is currently linked
- use `x-posts` when the user wants their recent posts or another account's public posts
- use `x-user` when the user wants a quick public profile lookup by handle
- use `x-home` when the user wants the authenticated home timeline
- use `x-package-create` when the user wants a draftable X article/thread package with tied image prompts
- use `x-content-show` when the user wants the full local package with article body, thread posts, and image briefs in one view
- use `x-media` when the user wants the stored imagery layer for a post package
- keep daily and weekly routines visible so the answer reflects your actual operating system, not just meetings
- rely on the built-in trace system when you want to study or train on agenda-selection behavior later

## Good future extension points

- expose the agenda command through MCP so every agent can call it consistently
- persist preference memory around ideal meeting blocks, buffer time, and relationship cadence
- add smarter mapping from Gmail senders to people and organizations
