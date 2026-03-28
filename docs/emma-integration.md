# Emma Integration

`life-ops` can now talk directly to the Emma developer API with the same global key registry used for OpenAI, xAI, and X.

By default it targets:

- base URL: `https://emma-sable.vercel.app`
- default agent: `soulbind`
- default mode: `listen`

## Setup

Store the Emma developer API key once:

```bash
zsh ./bin/life-ops keys-set --name EMMA_API_KEY --value "emma_live_..."
```

## Commands

```bash
zsh ./bin/life-ops emma-status
zsh ./bin/life-ops emma-me
zsh ./bin/life-ops emma-agents
zsh ./bin/life-ops emma-chat --agent soulbind --message "Talk to me about what you think I've been holding lately."
```

You can override the deployment URL when needed:

```bash
zsh ./bin/life-ops emma-me --base-url http://localhost:3000
```

## Notes

- `emma-me` and `emma-agents` are read-only.
- `emma-chat` writes a real conversation turn into Emma memory, so use it intentionally.
- The current CLI sends one user message per call. If we want richer threaded conversations later, we can extend it to accept a JSON message bundle.
