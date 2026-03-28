# Global Keys

`life-ops` can keep a small global key registry outside the repo so API keys do not need to be manually exported every time.

## What it does

- stores named secrets globally for `life-ops`
- auto-loads registered secrets whenever `life-ops` starts
- can print shell `export` lines for broader session activation
- defaults to macOS Keychain on macOS
- blocks plaintext file-backed secret storage unless you explicitly opt in
- provides the shared master key used for encrypted local DB storage, encrypted mail vault files, and encrypted backups

## Core commands

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops keys-set --name OPENAI_API_KEY --value "your-key-here"
zsh ./bin/life-ops keys-list
zsh ./bin/life-ops keys-export
eval "$(zsh ./bin/life-ops-env)"
```

## Recommended usage

If your current shell already has a key exported, capture it without pasting the value again:

```bash
zsh ./bin/life-ops keys-set --name OPENAI_API_KEY --from-env
```

If you intentionally want a simple plaintext fallback instead of macOS Keychain:

```bash
zsh ./bin/life-ops keys-set --name OPENAI_API_KEY --value "your-key-here" --backend file --allow-insecure-file-backend
```

## Notes

- `keys-list` never prints secret values
- `keys-export` prints `export ...` lines, so use it with `eval` or `source`
- the registry lives outside the repo at `~/.config/life-ops/keys.json`
- plaintext file-backed secrets are an explicit escape hatch, not the default safe path
