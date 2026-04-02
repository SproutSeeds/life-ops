from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OutreachIssue:
    level: str
    message: str


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().split())


def _contains_phrase(text: str, phrase: str) -> bool:
    return _normalize_text(phrase) in _normalize_text(text)


def _draft_similarity(left: str, right: str) -> float:
    return SequenceMatcher(a=_normalize_text(left), b=_normalize_text(right)).ratio()


def validate_outreach_manifest(manifest_path: Path) -> dict[str, Any]:
    manifest = _load_json(manifest_path)
    base_dir = manifest_path.parent
    project = manifest.get("project", {})
    rules = manifest.get("rules", {})
    recipients = manifest.get("recipients", [])
    issues: list[OutreachIssue] = []
    summaries: list[dict[str, Any]] = []

    required_links = [str(link) for link in project.get("links", [])]
    install_command = str(project.get("install_command", "")).strip()
    forbidden_phrases = [str(item) for item in rules.get("forbidden_phrases", [])]
    require_human_agent_language = bool(rules.get("require_human_agent_language", False))
    min_distinctives = int(rules.get("min_distinctives", 2))
    max_similarity = float(rules.get("max_similarity", 0.9))

    if not recipients:
        issues.append(OutreachIssue("error", "manifest must include at least one recipient"))

    if bool(rules.get("require_project_links", True)) and not required_links:
        issues.append(OutreachIssue("error", "manifest must declare project links"))

    if bool(rules.get("require_install_command", True)) and not install_command:
        issues.append(OutreachIssue("error", "manifest must declare an install_command"))

    rendered_drafts: list[tuple[str, str]] = []

    for recipient in recipients:
        recipient_id = str(recipient.get("id", "")).strip() or "unknown"
        draft_path = base_dir / str(recipient.get("draft_path", "")).strip()
        salutation = str(recipient.get("salutation", "")).strip()
        title_source = str(recipient.get("title_source", "")).strip()
        must_include = [str(item) for item in recipient.get("must_include", [])]
        distinctives = [str(item) for item in recipient.get("distinctives", [])]

        if not draft_path.exists():
            issues.append(OutreachIssue("error", f"{recipient_id}: missing draft file {draft_path}"))
            continue

        if not salutation:
            issues.append(OutreachIssue("error", f"{recipient_id}: missing salutation"))

        if not title_source:
            issues.append(OutreachIssue("error", f"{recipient_id}: missing official title_source"))

        if len(distinctives) < min_distinctives:
            issues.append(
                OutreachIssue(
                    "error",
                    f"{recipient_id}: expected at least {min_distinctives} distinctives, found {len(distinctives)}",
                )
            )

        draft_text = draft_path.read_text(encoding="utf-8").strip()
        if not draft_text:
            issues.append(OutreachIssue("error", f"{recipient_id}: draft file is empty"))
            continue

        if not draft_text.startswith("Subject:"):
            issues.append(OutreachIssue("error", f"{recipient_id}: draft must start with 'Subject:'"))

        if salutation and salutation not in draft_text:
            issues.append(OutreachIssue("error", f"{recipient_id}: missing salutation '{salutation}' in draft"))

        if install_command and install_command not in draft_text:
            issues.append(OutreachIssue("error", f"{recipient_id}: missing install command in draft"))

        for link in required_links:
            if link not in draft_text:
                issues.append(OutreachIssue("error", f"{recipient_id}: missing project link '{link}'"))

        for phrase in must_include:
            if not _contains_phrase(draft_text, phrase):
                issues.append(OutreachIssue("error", f"{recipient_id}: missing required phrase '{phrase}'"))

        for phrase in forbidden_phrases:
            if _contains_phrase(draft_text, phrase):
                issues.append(OutreachIssue("error", f"{recipient_id}: contains forbidden phrase '{phrase}'"))

        if require_human_agent_language:
            normalized = _normalize_text(draft_text)
            if "humans and agents" not in normalized and not ("human" in normalized and "agent" in normalized):
                issues.append(
                    OutreachIssue(
                        "error",
                        f"{recipient_id}: draft must explicitly speak to both humans and agents",
                    )
                )

        rendered_drafts.append((recipient_id, draft_text))
        summaries.append(
            {
                "id": recipient_id,
                "draft_path": str(draft_path),
                "subject": draft_text.splitlines()[0].replace("Subject:", "", 1).strip(),
                "title_source": title_source,
            }
        )

    for index, (left_id, left_text) in enumerate(rendered_drafts):
        for right_id, right_text in rendered_drafts[index + 1 :]:
            similarity = _draft_similarity(left_text, right_text)
            if similarity > max_similarity:
                issues.append(
                    OutreachIssue(
                        "error",
                        f"{left_id} and {right_id}: drafts are too similar ({similarity:.3f} > {max_similarity:.3f})",
                    )
                )

    return {
        "manifest_path": str(manifest_path),
        "project_name": project.get("name"),
        "recipient_count": len(recipients),
        "drafts": summaries,
        "issues": [{"level": issue.level, "message": issue.message} for issue in issues],
        "ok": not any(issue.level == "error" for issue in issues),
    }


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate a recipient-aware academic outreach manifest.")
    parser.add_argument("--manifest", type=Path, required=True, help="Path to the outreach manifest JSON file.")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def main() -> int:
    parser = _build_argument_parser()
    args = parser.parse_args()
    result = validate_outreach_manifest(args.manifest)

    if args.format == "json":
        print(json.dumps(result, indent=2))
    else:
        if result["ok"]:
            print("OUTREACH_VALIDATION_OK")
            print(f"- project: {result['project_name']}")
            print(f"- recipients: {result['recipient_count']}")
            for draft in result["drafts"]:
                print(f"- {draft['id']}: {draft['subject']} ({draft['draft_path']})")
        else:
            print("OUTREACH_VALIDATION_FAILED")
            for issue in result["issues"]:
                print(f"- {issue['level']}: {issue['message']}")

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
