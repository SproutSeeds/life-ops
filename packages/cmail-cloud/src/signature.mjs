// Signature decoration comes only from deployed identity configuration. Message
// text stays editable and is always retained verbatim as the plain-text part.
export function signatureLogo(identity) {
  const logo = identity?.signature_logo;
  if (!logo || typeof logo.url !== 'string' || typeof logo.href !== 'string') return null;
  try {
    const url = new URL(logo.url), href = new URL(logo.href);
    if ([url, href].some((v) => v.protocol !== 'https:' || v.username || v.password || v.port || v.search || v.hash) ||
        url.origin !== href.origin || !/^[a-z0-9:/._~%+-]+$/i.test(url.href) || !/\.(png|jpe?g)$/i.test(url.pathname) || url.href.length > 1024 || href.href.length > 1024) return null;
    return { url: url.href, href: href.href, alt: String(logo.alt || 'Company logo').slice(0, 160) };
  } catch { return null; }
}

export function splitSignature(text, signature) {
  const body = String(text || '').trimEnd(), suffix = String(signature || '').trimEnd();
  if (!suffix || !body.endsWith(suffix)) return null;
  const start = body.length - suffix.length;
  if (start && body[start - 1] !== '\n') return null;
  return { body: body.slice(0, start).trimEnd(), signature: suffix };
}

export function publicIdentity(address, identity) {
  const logo = signatureLogo(identity);
  return { address, from: identity.from, signature: String(identity.signature || ''), ...(logo ? { signature_logo: logo } : {}) };
}

const escape = (value) => String(value).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
const link = (url, label) => `<a href="${escape(url)}" style="color:#004b36;text-decoration:none">${escape(label)}</a>`;

export function signatureHtml(identity, text) {
  const logo = signatureLogo(identity), parts = splitSignature(text, identity?.signature);
  if (!logo || !parts) return '';
  const lines = parts.signature.split('\n').map((line, n) => {
    if (/^https:\/\/[^\s<>"']+$/i.test(line)) {
      try { const url = new URL(line); if (!url.username && !url.password) return link(url.href, line.replace(/^https:\/\//, '').replace(/\/$/, '')); } catch {}
    }
    if (/^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}$/i.test(line)) return link('mailto:' + line, line);
    if (/^\(\d{3}\) \d{3}-\d{4}$/.test(line)) return link('tel:+1' + line.replace(/\D/g, ''), line);
    return n === 0 ? `<strong style="font-size:16px;color:#004b36">${escape(line)}</strong>` : escape(line);
  }).join('<br>');
  return `<table role="presentation" cellpadding="0" cellspacing="0" border="0" style="font:13px/1.6 Arial,Helvetica,sans-serif;color:#203a30"><tr><td style="vertical-align:middle;padding:0 18px 0 0"><a href="${escape(logo.href)}"><img src="${escape(logo.url)}" alt="${escape(logo.alt)}" width="96" height="96" style="display:block;width:96px;height:96px;border:0"></a></td><td style="vertical-align:middle;border-left:2px solid #cf4d1c;padding-left:18px">${lines}</td></tr></table>`;
}

export function brandedMessage(identity, text) {
  const footer = signatureHtml(identity, text), parts = splitSignature(text, identity?.signature);
  if (!footer || !parts) return '';
  return `<!doctype html><html><body><div style="font:15px/1.6 Arial,Helvetica,sans-serif;color:#202520">${escape(parts.body).replace(/\r?\n/g, '<br>')}</div><div style="margin-top:24px">${footer}</div></body></html>`;
}
