function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeRecipient(recipient) {
  if (typeof recipient === "string") {
    const email = recipient.trim();
    if (!email) {
      throw new Error("Email recipient strings cannot be empty.");
    }
    return { email, name: null };
  }
  if (!recipient || typeof recipient !== "object") {
    throw new Error("Expected an email recipient object or email string.");
  }
  const email = String(recipient.email ?? "").trim();
  if (!email) {
    throw new Error("Email recipients require an email address.");
  }
  const name = recipient.name == null ? null : String(recipient.name).trim() || null;
  return { email, name };
}

function normalizeRecipients(recipients = []) {
  if (!Array.isArray(recipients)) {
    throw new Error("Expected recipients to be an array.");
  }
  return recipients.map((recipient) => normalizeRecipient(recipient));
}

function normalizeSections(sections = []) {
  if (!Array.isArray(sections)) {
    throw new Error("Expected sections to be an array.");
  }
  return sections
    .map((section) => {
      if (!section || typeof section !== "object") {
        return null;
      }
      const heading = String(section.heading ?? "").trim();
      const body = String(section.body ?? "").trim();
      const bullets = Array.isArray(section.bullets)
        ? section.bullets.map((bullet) => String(bullet).trim()).filter(Boolean)
        : [];
      if (!heading && !body && bullets.length === 0) {
        return null;
      }
      return { heading, body, bullets };
    })
    .filter(Boolean);
}

export function renderEmailText(draft) {
  const lines = [];

  if (draft.intro) {
    lines.push(draft.intro);
    lines.push("");
  }

  for (const section of draft.sections) {
    if (section.heading) {
      lines.push(section.heading);
    }
    if (section.body) {
      lines.push(section.body);
    }
    for (const bullet of section.bullets) {
      lines.push(`- ${bullet}`);
    }
    lines.push("");
  }

  if (draft.cta) {
    lines.push(draft.cta);
    lines.push("");
  }

  if (draft.closing) {
    lines.push(draft.closing);
  }

  return lines.join("\n").trim();
}

export function renderEmailHtml(draft) {
  const sectionMarkup = draft.sections
    .map((section) => {
      const parts = [];
      if (section.heading) {
        parts.push(`<h2>${escapeHtml(section.heading)}</h2>`);
      }
      if (section.body) {
        parts.push(`<p>${escapeHtml(section.body)}</p>`);
      }
      if (section.bullets.length > 0) {
        parts.push(
          `<ul>${section.bullets.map((bullet) => `<li>${escapeHtml(bullet)}</li>`).join("")}</ul>`,
        );
      }
      return parts.join("");
    })
    .join("");

  return [
    "<!doctype html>",
    "<html>",
    "<body>",
    draft.intro ? `<p>${escapeHtml(draft.intro)}</p>` : "",
    sectionMarkup,
    draft.cta ? `<p>${escapeHtml(draft.cta)}</p>` : "",
    draft.closing ? `<p>${escapeHtml(draft.closing).replaceAll("\n", "<br />")}</p>` : "",
    "</body>",
    "</html>",
  ].join("");
}

export function draftStructuredEmail({
  to = [],
  cc = [],
  bcc = [],
  subject,
  previewText = "",
  intro = "",
  sections = [],
  cta = "",
  closing = "Thanks,",
  metadata = {},
} = {}) {
  const normalizedSubject = String(subject ?? "").trim();
  if (!normalizedSubject) {
    throw new Error("Structured emails require a subject.");
  }

  const draft = {
    to: normalizeRecipients(to),
    cc: normalizeRecipients(cc),
    bcc: normalizeRecipients(bcc),
    subject: normalizedSubject,
    previewText: String(previewText).trim(),
    intro: String(intro).trim(),
    sections: normalizeSections(sections),
    cta: String(cta).trim(),
    closing: String(closing).trim(),
    metadata: metadata && typeof metadata === "object" ? { ...metadata } : {},
  };

  return {
    ...draft,
    text: renderEmailText(draft),
    html: renderEmailHtml(draft),
  };
}

export async function sendEmailDraft({ draft, sender, context = {} }) {
  if (!sender || typeof sender.send !== "function") {
    throw new Error("sendEmailDraft requires a sender with a send(payload) method.");
  }
  return sender.send({
    ...draft,
    context,
  });
}
