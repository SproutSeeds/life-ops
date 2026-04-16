# Homemade Calendar

Life Ops includes a local-first calendar tracking layer that sits beside mail, agenda, routines, and lists. It is intentionally not a vendor calendar clone. It is a day-by-day operating log for what was planned, what actually happened, what did not happen, and what should be carried forward.

## Mental Model

The calendar has four layers:

- `calendar_entries`: dated tasks, notes, memories, habits, milestones, and carry-forward items.
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
