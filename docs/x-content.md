# X Content Studio

`life-ops` can now keep local X article/thread packages and image briefs in SQLite, then optionally render images for those briefs with the OpenAI Images API.

## What it does

- stores article drafts for X-led content campaigns
- stores the supporting thread posts under the same parent package
- stores image briefs and prompts tied to that package
- optionally generates actual image files into `data/x_media/`

## Core flow

```bash
cd /Volumes/Code_2TB/code/life-ops
zsh ./bin/life-ops x-package-create \
  --title "Define Your Canonical Dossier" \
  --angle "turn scattered life-admin into a searchable operating system" \
  --thesis "your memory should not be the only database keeping your life together" \
  --point "Capture the truth layer first" \
  --point "Separate active queue from long-term archive" \
  --point "Turn records into actual operating leverage"

zsh ./bin/life-ops x-content
zsh ./bin/life-ops x-content-show --id 1
zsh ./bin/life-ops x-media --content-id 1
```

## Image generation

Set `OPENAI_API_KEY` locally first, then generate a stored image brief:

```bash
zsh ./bin/life-ops keys-set --name OPENAI_API_KEY --from-env
zsh ./bin/life-ops x-generate-image --asset-id 1
```

If the key is not already in your current shell, you can store it directly:

```bash
zsh ./bin/life-ops keys-set --name OPENAI_API_KEY --value "your-key-here"
```

Generated files are saved under `data/x_media/`.

For xAI / Grok image generation, save your xAI key once and use the xAI provider:

```bash
zsh ./bin/life-ops keys-set --name XAI_API_KEY --value "your-xai-key"
zsh ./bin/life-ops x-generate-image --asset-id 1 --provider xai --model grok-imagine-image --resolution 2k
```

If both keys are available, the default `--provider auto` path now prefers xAI first and automatically falls back to OpenAI if xAI returns an upstream error:

```bash
zsh ./bin/life-ops x-generate-image --asset-id 1
```

## Notes

- `x-package-create` is deterministic first-pass packaging, not full AI writing.
- the image-generation step is optional and can use either `OPENAI_API_KEY` or `XAI_API_KEY`
- `x-generate-image --provider auto` prefers xAI and falls through to OpenAI when xAI is blocked or unavailable
- generated images are stored locally and tracked in the same SQLite database
