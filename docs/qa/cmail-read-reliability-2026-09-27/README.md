# CMAIL read reliability deployment — September 27, 2026

CMAIL intermittently dropped UI requests with `sqlite3.OperationalError: unable
to open database file`. The previous service log held 3,366 of them: 3,201 on
the sync-status poll, 158 when opening a message, and 7 on other reads. A
dropped message request surfaced in the browser as "Cmail is private. Open
Tailscale…". This deployment fixes the cause and also ships the checkout's
malformed Message-ID ingest fix.

## Cause

Every failure came from the first query on a UI read connection opened with
`file:…?mode=ro`. Scheduled jobs run checkout code on uv Python with bundled
SQLite 3.50.4, which deletes the `-wal` and `-shm` files when its last
connection closes. CMAIL runs pyenv Python on macOS SQLite 3.51.0, whose
file-level read-only connections cannot recreate those files, so SQLite
returns `SQLITE_CANTOPEN` with errno 2 until a writer recreates them.

Evidence (`VERIFICATION.json`):

- A 150-second probe of the live database with the server's SQLite library:
  read-only opens failed 379 of 4,829 times, each at the first query with
  errno 2 while both side files were absent; read-write opens with
  `query_only` failed 0 of 5,468 times.
- A deterministic reproduction across the two SQLite builds.
- Live end-to-end, 60 rounds of a scheduled-job-style open and close followed
  by a sync-status request and a message request: before the fix 1 and 2
  requests succeeded and the log gained 118 tracebacks; after the fix all 120
  succeeded and the log gained none.

## Fix

`_MailUiSnapshotCache.get_connection()` opens the live runtime database with
`mode=rw`, keeping `PRAGMA query_only = ON`. SQLite can rebuild the WAL side
files, SQL writes stay blocked, and a missing database is never created.
`tests.test_cmail_read_connections` covers write blocking, the file-level
open mode (it fails against the previous code) and the missing-database case.

`mail_ingest.py` now reads Message-ID, In-Reply-To and References from the raw
headers, so a malformed Message-ID no longer stops ingestion. No production
log showed that failure; an installed-code smoke test ingested a malformed
Message-ID into a temporary database.

## Deployment

The runtime was sealed and encrypted backup `life-ops-db-20260927T162612Z` was
created with the installed CMAIL environment. `mail_ui.py` and
`mail_ingest.py` in the ignored backend bundle now equal the checkout;
`PRODUCTION.patch` is the exact change against the previously installed
modules and `DEPLOYMENT.json` records hashes and rollback. The environment was
rebuilt with the usual `pip install --no-index --no-deps --no-build-isolation
--force-reinstall ./packages/lifeops-cli/backend`; every installed module
matched the bundle. Only the CMAIL launchd job was restarted.

## Verification

- 137 bundle tests passed, including the malformed Message-ID ingest test and
  the new read-connection tests. The checkout suite ran 659 tests with the one
  unrelated scheduled-run count failure noted in the labels record.
- After the restart: health passed locally and on the private route, the
  ClawDad label still held 17 messages, and the service log stayed clean.
