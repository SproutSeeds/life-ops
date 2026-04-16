# Apple Calendar Feed Sync

Life Ops can import a read-only Apple/iCloud Calendar feed from a private `.ics` or `webcal://` URL. Imported Apple events land in the normal `events` table, so they automatically appear in:

- `agenda`
- `calendar-day`
- saved daily snapshots
- the `/calendar` Life Ops UI

This does not edit Apple Calendar. Apple remains the source of truth for event changes, and Life Ops refreshes its local copy whenever you run the sync command.

## Setup

Create or copy an iCloud Calendar public feed URL, then keep it secret. Anyone with the URL can read that calendar.

On iCloud.com, sign in to Calendar, open the calendar's info/share control, turn on Public Calendar, and copy the link. On macOS Calendar, show the calendar list, open sharing for the iCloud calendar, enable Public Calendar, then copy/share the link.

Put the URL in a local ignored file:

```bash
printf '%s\n' 'webcal://your-private-icloud-calendar-feed' > config/apple_calendar.url
```

Sync it into Life Ops:

```bash
zsh ./bin/life-ops sync-apple-calendar \
  --url-file config/apple_calendar.url \
  --calendar-name "Personal iCloud"
```

If you exported or downloaded a local `.ics` file instead, use:

```bash
zsh ./bin/life-ops sync-apple-calendar \
  --file ~/Downloads/calendar.ics \
  --calendar-name "Personal iCloud"
```

## Daily Use

Refresh the feed, then ask Life Ops for the day:

```bash
zsh ./bin/life-ops sync-apple-calendar --url-file config/apple_calendar.url
zsh ./bin/life-ops calendar-day --date 2026-04-14
```

Or ask for the agent-readable JSON payload:

```bash
zsh ./bin/life-ops calendar-day --date 2026-04-14 --format json
```

## Import Behavior

- Feed URLs starting with `webcal://` are fetched over `https://`.
- The default import window is 30 days back and 365 days ahead.
- Re-syncing a feed replaces prior Life Ops events for that same Apple feed, which removes stale deleted events.
- Basic recurrence rules are expanded locally for `DAILY`, `WEEKLY`, `MONTHLY`, and `YEARLY`; weekly `BYDAY`, `COUNT`, `UNTIL`, and `EXDATE` are supported.
- Imported event source is `apple-calendar-feed`, and imported event kind is `apple-calendar`.

## Privacy

Do not commit the feed URL. It is effectively a read-only calendar secret.

If the URL leaks, turn off public calendar sharing in Apple/iCloud Calendar, then generate a new sharing link and update `config/apple_calendar.url`.

Apple setup references:

- <https://support.apple.com/en-euro/guide/icloud/-mm6b1a9479/icloud>
- <https://support.apple.com/guide/calendar/share-icloud-calendars-icl32362/mac>
