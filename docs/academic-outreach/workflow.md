# Academic Outreach Workflow

This repo now has a reusable, ORP-gated workflow for research-adjacent outreach where:

- the recipient's current role/title is verified from an official source
- product claims are checked against the live repo and npm package
- each recipient gets a distinct draft instead of a mass-template note
- the draft is reviewed as plain text before any live send

## Why ORP did not catch this earlier

`orp` was installed globally, but `life-ops` did not have an ORP config dedicated to academic outreach. The repo had share-draft primitives, but not a gate that enforced:

- recipient-specific differentiation
- official title verification
- provenance and contribution wording checks
- anti-hedge language checks

That gap is now covered by [orp.academic-outreach.yml](/Volumes/Code_2TB/code/life-ops/orp.academic-outreach.yml).

## Files

- manifest: [manifest.json](/Volumes/Code_2TB/code/life-ops/examples/academic-outreach/manifest.json)
- Tao draft: [tao.txt](/Volumes/Code_2TB/code/life-ops/examples/academic-outreach/tao.txt)
- Bloom draft: [bloom.txt](/Volumes/Code_2TB/code/life-ops/examples/academic-outreach/bloom.txt)
- validator script: [validate_academic_outreach.py](/Volumes/Code_2TB/code/life-ops/scripts/validate_academic_outreach.py)
- validation module: [outreach_validation.py](/Volumes/Code_2TB/code/life-ops/src/life_ops/outreach_validation.py)

## Workflow

1. Verify the recipient's current role from an official source.
2. Verify project claims against the live repo and npm package.
3. Define shared claims once in the manifest.
4. Write one plain-text draft per recipient.
5. Add recipient-specific `must_include` phrases and `distinctives`.
6. Run the validator locally.
7. Run the ORP gate and emit a packet.
8. Send a self-test through the sovereign mail stack before sending live.

## Validation commands

Run the validator directly:

```bash
python3 scripts/validate_academic_outreach.py \
  --manifest examples/academic-outreach/manifest.json
```

Run the ORP gate:

```bash
orp --config orp.academic-outreach.yml \
  gate run --profile academic_outreach_default --json
```

Emit the ORP packet after a passing run:

```bash
orp --config orp.academic-outreach.yml \
  packet emit --profile academic_outreach_default --json
```

## Suggested drafting rules

- Prefer plain text for serious mathematical outreach.
- Keep gratitude explicit, but do not weaken the note with apology language.
- State clearly that the system is built for both humans and agents to use.
- Keep the upstream record visible in the wording.
- Ask for structure-level reaction, not blanket approval.
- Avoid sending the exact same message to multiple recipients.

## Current example distinction

The current examples separate the angles deliberately:

- Tao draft: frames the project against usable AI tools for research and puts it on his radar.
- Bloom draft: frames the project as building on the erdosproblems.com ecosystem and asks for a reaction to the structure from that vantage point.

## Sending after validation

After the gate passes, send a self-test through the local sovereign mail stack first:

```bash
zsh ./bin/life-ops resend-send-email \
  --to codyshanemitchell@gmail.com \
  --subject "Text draft: erdos-problems note for Terence Tao" \
  --text "paste validated draft here"
```
