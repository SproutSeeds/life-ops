# CMAIL Agent

The CMAIL header's **Agent** button opens a dedicated, persistent Codex thread in macOS Terminal. The thread always starts in the Life Ops checkout and receives the currently selected CMAIL item as a pointer-only handoff.

The launch is intentionally prompt-free. After Terminal opens, the Codex composer is blank and ready for Cody's request. Repository `AGENTS.md` instructions provide the stable CMAIL operating context.

## Starting a CMAIL task

1. Read `~/.lifeops/state/cmail-agent.json` when Cody refers to “this email,” “this draft,” or the item selected in CMAIL.
2. Treat the state file as a pointer. Resolve the selected `communicationId` or `draftId` from the live CMAIL API or database before drawing conclusions.
3. Read [DATA_MAP.md](DATA_MAP.md) for the canonical mail surfaces and [RUNBOOK.md](RUNBOOK.md) for launch diagnostics.
4. Preserve normal mail safety boundaries. Reviewing mail is read-only; drafting, sending, deleting, classifying, or changing follow-up state requires the authority present in Cody's request.

The state file contains only project/session metadata and safe selection identifiers. It does not copy subjects, bodies, recipient lists, attachment content, credentials, or ClawDad's bearer token.

## Correspondence voice

An edit, update, or rewrite request covers existing editable messages. If a delivered message cannot be edited, report that limitation and stop there. Sending a replacement, correction, apology, or follow-up requires Cody's explicit request for that new send; broad outreach permission does not authorize a resend as a workaround for an edit request.

Draft and send authorized correspondence in Cody Mitchell's first-person voice. Get straight to the purpose; introduce Cody only when useful. Close as Cody or Cody Mitchell using the established signature, and avoid duplicating a signature CMAIL supplies.

Introduce or sign as Codex, Claude, an AI assistant, or an agent assisting or writing on behalf of Cody only when Cody explicitly requests that attribution for the particular message. Keep relevant AI-workflow explanations in Cody's voice and keep claims accurate. Check the opening, body, and closing before presenting or sending. Preserve exact historical sent records; older agent-voiced messages do not set the style for new correspondence.

## Session behavior

- The ClawDad session title is `CMAIL Agent`.
- A valid existing session is resumed on every click.
- If the stored session is missing or has an invalid project binding, the launcher creates a replacement and updates the state file.
- Creating the CMAIL session temporarily changes ClawDad's active-session pointer; the launcher restores the previously active Life Ops session before opening Terminal.
- The first launch of a new placeholder starts Codex in the project. ClawDad's Codex integration then binds the resulting native Codex thread for later resume launches.

## Selection state

The file uses schema `life_ops.cmail_agent/1` and has this shape:

```json
{
  "project": "/Volumes/Code_2TB/code/life-ops",
  "schema": "life_ops.cmail_agent/1",
  "selection": {
    "communicationId": 123,
    "contactKey": "contact@example.com",
    "identity": "frg",
    "messageId": "<provider-message-id>",
    "threadKey": "<provider-thread-id>",
    "view": "inbox"
  },
  "sessionId": "native-codex-session-id",
  "updatedAt": "UTC timestamp"
}
```

A draft selection uses `draftId` in place of `communicationId`. With no selected row, `selection` still records the current view and identity.
