# X Integration

`life-ops` now has a real local X auth and command surface, so you can connect your account and manage reads/posts from the same repo.

For content packaging and media prompts on top of that auth layer, see `docs/x-content.md`.

## What we want

- read your own recent and historical posts
- read your home timeline and mentions
- inspect public posts from other accounts
- eventually draft, queue, and publish posts from the same local-first system

## Current local setup

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops x-status
zsh ./bin/life-ops x-auth
zsh ./bin/life-ops x-me
zsh ./bin/life-ops x-posts --limit 5
zsh ./bin/life-ops x-user --username XDevelopers
zsh ./bin/life-ops x-home --limit 10
zsh ./bin/life-ops x-post --text "hello from life-ops"
```

- `x-init-config` writes a local template at `config/x_client.json` if you need to start from scratch
- `x-status` shows whether local config and token files are present and whether the current scopes are enough for reading and posting
- `x-auth` runs the local OAuth callback flow and stores the token in `data/x_token.json`
- `x-me` confirms which account is linked
- `x-posts` reads your recent posts or another account's posts
- `x-user` looks up another public account by username
- `x-home` reads your authenticated home timeline
- `x-post` and `x-delete-post` publish or remove your own posts
- `x-package-create`, `x-content`, `x-content-show`, `x-media`, and `x-generate-image` cover the local-first content-studio layer for articles, thread drafts, and imagery

The example config is in `config/x_client.example.json`. The real local config is `config/x_client.json`, which should stay uncommitted.

## Recommended first-pass scopes

For the user-account flow we want:

- `tweet.read`
- `users.read`
- `tweet.write`
- `offline.access`

That covers reading your own account data, reading timelines with user context, posting, and keeping a refresh token for long-lived access.

For public read-only lookups, a bearer token can still be useful even before full user auth is wired.

## What is live now

1. Auth and account identity
   Store app config and token metadata locally, then confirm the linked X account with `/2/users/me`.

2. Read layer
   Read your authored posts, another user's public account record, and your home timeline.

3. Compose layer
   Publish and delete your own posts from the CLI.

## Next phases

1. Local persistence
   Save pulled posts and timeline slices into SQLite so they become part of the broader life-ops memory.

2. Mentions and engagement
   Add mentions, replies, likes, bookmarks, and queue-aware post workflows.

3. Creative memory
   Treat X posts and threads as part of your long-term creative and public-history context.

## Official docs used for this roadmap

- OAuth 2.0 Authorization Code with PKCE: https://docs.x.com/resources/fundamentals/authentication/oauth-2-0/user-access-token
- Authentication overview: https://docs.x.com/fundamentals/authentication/overview
- X API overview: https://docs.x.com/x-api/getting-started/about-x-api
- Rate limits: https://docs.x.com/x-api/fundamentals/rate-limits
- User timeline endpoint: https://docs.x.com/x-api/users/get-timeline
