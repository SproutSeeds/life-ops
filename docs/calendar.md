# Homemade Calendar

Life Ops includes a local-first calendar tracking layer that sits beside mail, agenda, routines, and lists. It is intentionally not a vendor calendar clone. It is a day-by-day operating log for what was planned, what actually happened, what did not happen, and what should be carried forward.

## Mental Model

The calendar has four layers:

- `calendar_entries`: dated tasks, notes, memories, habits, milestones, carry-forward items, and optional recurrence anchors.
- `calendar_day_notes`: one mutable note record per day for intention, reflection, mood, energy, and loose notes.
- `calendar_day_snapshots`: immutable historic saves of the day view.
- `agenda`: the existing live view of routines, events, and communication follow-ups.

The important rule is that a day can be saved. Once saved, the snapshot preserves what Life Ops knew that day, including done work, unfinished work, agenda context, open lists, and notes.

## Daily Flow

Morning:

```bash
zsh ./bin/life-ops calendar-note \
  --date 2026-04-14 \
  --intention "Make the calendar system real" \
  --energy "medium-high"

zsh ./bin/life-ops calendar-add \
  --date 2026-04-14 \
  --title "Build Life Ops calendar history" \
  --type task \
  --priority high \
  --start-time 09:30 \
  --tag life-ops
```

During the day:

```bash
zsh ./bin/life-ops calendar-done --id 12
zsh ./bin/life-ops calendar-status --id 13 --status missed
zsh ./bin/life-ops calendar-add --date 2026-04-14 --title "Unexpected call with Alex" --type memory --status done
```

Evening:

```bash
zsh ./bin/life-ops calendar-note \
  --date 2026-04-14 \
  --reflection "Calendar shipped; rollover still needs tuning."

zsh ./bin/life-ops calendar-save-day \
  --date 2026-04-14 \
  --summary "Calendar foundation shipped; one item carried forward."
```

Next morning:

```bash
zsh ./bin/life-ops calendar-rollover --from 2026-04-14 --to 2026-04-15
```

## Views

Show one day:

```bash
zsh ./bin/life-ops calendar-day --date 2026-04-14
zsh ./bin/life-ops calendar-day --date 2026-04-14 --format json
```

Show the always-forward planning horizon:

```bash
zsh ./bin/life-ops calendar-range --start 2026-04-24 --days 365
```

Add a repeating entry:

```bash
zsh ./bin/life-ops calendar-add \
  --date 1991-06-12 \
  --title "Cody birthday" \
  --type event \
  --repeat yearly \
  --repeat-anchor 1991-06-12 \
  --tag birthday
```

The CMAIL `/calendar` tab renders the same 365-day range from the active CMAIL
runtime database and expands daily, weekly, monthly, and yearly recurrence
rules from their anchor date.

Print a day sheet:

```bash
zsh ./bin/life-ops day-sheet --date 2026-04-14
zsh ./bin/life-ops day-sheet --date 2026-04-14 --page-breaks
zsh ./bin/life-ops day-sheet --date 2026-04-14 --format html --output /tmp/lifeops-day-sheet.html
```

Sweep ORP project priorities before printing:

```bash
zsh ./bin/life-ops orp-sweep --date 2026-04-14 --update-calendar
zsh ./bin/life-ops orp-sweep --date 2026-04-14 --dry-run-calendar
```

The day sheet is the printable operating view. It starts with a canonical
The printed sheet leads with schedule and holds. It groups:

- hard schedule: timed events, calls, and dated commitments
- signups / bookings: FRG booking webhook holds and other tagged signup work
- calendar holds / bookings: active future timed holds, calls, and FRG bookings
- non-project personal/admin items that still belong on today's sheet

Broad project task inventory is intentionally omitted from the print body.
Instead, the sheet prints exactly one featured project with a compact abstract
and open-ended question. Change that project any time before the print by
setting `LIFEOPS_DAY_SHEET_FEATURED_PROJECT` or writing the project title to:

`~/.lifeops/config/day-sheet-featured-project.txt`

If no project is configured, LifeOps auto-selects the strongest recent project
signal from the last seven days.

Active meeting bookings from the FRG site are printed in the calendar-hold
section regardless of how far out they are scheduled, so a paid or confirmed
consultation never disappears just because it falls outside a normal planning
window.

The FRG booking handoff is a signed receiver at `POST /api/frg/bookings`.
Production FRG site bookings should set `FRG_BOOKING_WEBHOOK_URL`,
`FRG_BOOKING_WEBHOOK_SECRET`, and `FRG_BOOKING_WEBHOOK_REQUIRED=1` so Stripe-paid
bookings fail loudly if they cannot create the LifeOps calendar hold.
The receiver defaults to calendar-first behavior: it records the booking in
LifeOps without creating a CMAIL draft. Set `FRG_BOOKING_CONFIRMATION_MODE=draft`
to keep generated confirmations for manual review, or
`FRG_BOOKING_CONFIRMATION_MODE=send` to queue confirmations automatically.

Forge conference-seat purchases use a separate signed receiver at
`POST /api/frg/forge`. Production Forge checkout should set
`FRG_FORGE_WEBHOOK_URL` to that route when it differs from the booking receiver.
If Forge-specific variables are absent, the FRG site can derive `/api/frg/forge`
from `FRG_BOOKING_WEBHOOK_URL` and sign with `FRG_BOOKING_WEBHOOK_SECRET`.
Paid Forge seats are printed in the day sheet as an `Upcoming Conferences`
section with the attendee email, Stripe session, and required fulfillment action
until the seat item is completed.
Forge confirmations follow the same opt-in pattern with
`FRG_FORGE_CONFIRMATION_MODE=draft` or `FRG_FORGE_CONFIRMATION_MODE=send`.

Smoke-test the signed handoff without using a real customer booking:

```bash
FRG_BOOKING_WEBHOOK_SECRET=... \
  python scripts/frg_booking_handoff_smoke.py \
  --url http://127.0.0.1:4311/api/frg/bookings \
  --db ~/.lifeops/data/cmail_runtime.db \
  --cleanup
```

The smoke script posts a signed `booking.paid` payload, verifies the
`frg_site_booking` / `stripe_checkout_sessions` calendar row, and can mark the
smoke row canceled after verification.

The scheduled ORP sweep is described in
`ops/com.sproutseeds.lifeops-orp-sweep.plist`. It writes reports under
`~/.codex/memories/lifeops-orp-sweep/` and adds generated `orp-project-sweep`
calendar entries for the day.

The scheduled 8:30 AM print is described in
`ops/com.sproutseeds.lifeops-noon-day-sheet-print.plist` and implemented by
`scripts/noon_day_sheet_print.sh`. At `8:30` local time it:

- reruns the GitHub action-plan sweep into LifeOps
- reruns the ORP project-priority sweep into LifeOps
- saves a print snapshot
- renders fresh PDF, HTML, and text day-sheet files under
  `~/.codex/memories/lifeops-day-sheet-print/`
- sends the compact black-and-white PDF sheet to the default macOS printer with
  `lp`

The print path writes `latest.pdf` for review and keeps `latest.txt` only as a
debug fallback. `latest.pdf` is canonical for agents asked to print the day
sheet. The PDF renderer is intentionally dense: it starts with the normal
LifeOps schedule, prints calendar holds/bookings next, and then prints the
single featured project.

Render the canonical day sheet manually:

```bash
zsh ./bin/life-ops day-sheet --date 2026-04-25 --format latex --output /tmp/day-sheet.tex
```

Render with a one-off featured project override:

```bash
zsh ./bin/life-ops day-sheet --date 2026-04-25 --featured-project longevity-research
```

The scheduled print reads the active CMAIL/LifeOps calendar database at
`~/.lifeops/data/cmail_runtime.db` by default. It does not pull from Google
Calendar unless those events have already been imported into the LifeOps/CMAIL
calendar.

Use this to test without printing:

```bash
zsh scripts/noon_day_sheet_print.sh --dry-run --skip-refresh --skip-save
```

Show a history range:

```bash
zsh ./bin/life-ops calendar-history --start 2026-04-14 --days 14
zsh ./bin/life-ops calendar-history --start 2026-04-14 --days 14 --format json
```

The text view is meant for humans. The JSON view is meant for agents and future local UI surfaces.

## Entry Statuses

Use these statuses to preserve truth instead of hiding drift:

- `planned`: intended for the day, not started.
- `in_progress`: actively underway.
- `done`: completed and counted as done.
- `missed`: did not happen and should be visible as missed.
- `deferred`: intentionally pushed forward.
- `canceled`: no longer relevant.
- `archived`: removed from normal day/history views.

## Rollover Discipline

`calendar-rollover` marks unfinished source-day entries as `deferred` and creates new `carry_forward` entries on the target day. This preserves the historic truth of the source day while giving the next day a clean action surface.
