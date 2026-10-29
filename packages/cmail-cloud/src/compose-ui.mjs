import { splitSignature } from './signature.mjs';

// Shared with the browser bundle so ranking/normalization can be verified with
// realistic ambiguous names without loading any private contact data in tests.
export function matchContacts(contacts, query, selected = []) {
  const normalize = (value) => String(value || '').normalize('NFKD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
  const text = normalize(query.trim());
  if (text.length < 2 || /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(text)) return { total: 0, items: [] };
  const excluded = new Set(selected.map((address) => address.toLowerCase()));
  const tokens = text.split(/\s+/);
  const matches = [];
  for (const contact of contacts) {
    if (excluded.has(contact.address)) continue;
    const address = normalize(contact.address), name = normalize(contact.name);
    if (!tokens.every((token) => address.includes(token) || name.includes(token))) continue;
    const score = (address.startsWith(text) ? 100 : 0) + (name.startsWith(text) ? 80 : 0) + (contact.sent ? 30 : 0) + Math.min(contact.sent || 0, 20);
    matches.push({ contact, score });
  }
  matches.sort((a, b) => b.score - a.score || b.contact.last_seen.localeCompare(a.contact.last_seen) || a.contact.address.localeCompare(b.contact.address));
  return { total: matches.length, items: matches.slice(0, 7).map((item) => item.contact) };
}

function composeApplication(findMatches, splitSignature) {
  const $ = (id) => document.getElementById(id);
  const key = 'cmail.compose.draft.v1';
  const persistent = document.body.dataset.cmailDrafts === '1';
  const email = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}$/i;
  let draft = null, current = null, opening = false, busy = false, saving = false, config = null;
  let contacts = [], contactsAt = 0, contactsPromise = null, contactsError = false, signedOut = false;
  const trigger = document.createElement('button');
  trigger.type = 'button'; trigger.id = 'composeButton'; trigger.className = 'cmail-reply-trigger cmail-compose-trigger'; trigger.textContent = 'Compose';
  $('mailbox').before(trigger);
  const openingNotice = document.createElement('span'); openingNotice.className = 'cmail-reply-status'; openingNotice.setAttribute('role', 'status'); openingNotice.hidden = true; trigger.after(openingNotice);
  try { draft = JSON.parse(sessionStorage.getItem(key) || 'null'); } catch {}
  const errors = {
    invalid_draft: 'This draft is too large or has an invalid field. Your changes are kept in this tab.',
    draft_conflict: 'This draft changed in another tab. Save a copy to preserve your changes separately.',
    draft_changed: 'The saved version differs. Save a copy to preserve these changes.',
    draft_closed: 'This draft has already been sent or discarded. Save a copy if you want to keep these changes.',
    draft_sending: 'This draft is being sent. Check its confirmation before editing.',
    draft_not_found: 'This draft is no longer available.',
    invalid_recipients: 'Enter up to 10 complete email addresses.', invalid_subject: 'Check the subject and try again.',
    invalid_reply: 'Write your message before sending.', empty_reply: 'Write your message above the signature.',
    sender_not_configured: 'Choose a configured From address.', reply_changed: 'The original send is being confirmed. Retry it before editing.',
    confirmation_expired: 'This earlier send needs a status check before it can be retried. Your draft is saved.',
    send_unconfirmed: 'The send could not be confirmed. Retry confirmation to check it safely.',
    send_rejected: 'The mail service did not accept this message. Your draft is saved and can be edited.',
    send_rate_limited: 'The mail service is busy. Your draft is saved; try again shortly.',
    archive_pending: 'Your email was sent. Retry confirmation to finish saving its Sent copy.',
    attachments_too_large: 'Attachments can total up to 20 MB per email. Remove a file or choose smaller files.',
    too_many_attachments: 'You can attach up to 50 files per email.',
    attachment_type_blocked: 'The email provider blocks this file type. Choose a different file format.',
    attachment_empty: 'This file is empty. Choose a file with content.',
    invalid_attachment_name: 'This filename is not supported. Rename the file and try again.',
    attachment_changed: 'This upload changed. Choose the file again.',
    attachment_not_found: 'An attachment is unavailable. Remove it and attach the file again.',
    attachment_unavailable: 'The attachment could not be uploaded. Your saved draft is safe; choose the file again to retry.',
  };
  const meaningful = () => draft && (draft.attachments?.length || draft.to?.length || draft.pending || draft.subject || draft.text?.trim() !== draft.signature?.trim());
  function updateTrigger() { trigger.textContent = opening ? 'Opening…' : meaningful() ? 'Continue draft' : 'Compose'; trigger.disabled = opening || busy || saving; }
  function save(changed = false) {
    if (signedOut || !draft) return;
    if (changed && persistent) draft.dirty = true;
    try { sessionStorage.setItem(key, JSON.stringify(draft)); }
    catch { status('Your draft is kept in this page. Keep this tab open until you send it.'); }
    updateTrigger();
  }
  function clear() { draft = null; try { sessionStorage.removeItem(key); } catch {} updateTrigger(); }
  function status(text, error = false) {
    if (!current) return;
    current.status.textContent = text; current.status.classList.toggle('cmail-reply-error', error);
  }
  async function api(path, body) {
    const response = await fetch('/cmail/api/' + path, body === undefined ? { credentials: 'same-origin', cache: 'no-store' } : {
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', 'X-Cmail-Action': path.startsWith('drafts') ? 'draft' : 'compose' }, body: JSON.stringify(body),
    });
    if (response.redirected || !response.headers.get('Content-Type')?.includes('application/json')) throw new Error('Your sign-in expired. Refresh to sign in; your draft is saved.');
    const data = await response.json();
    if (!response.ok) throw Object.assign(new Error(errors[data.error] || 'Temporarily unavailable. Your draft is kept in this tab.'), { code: data.error, status: response.status });
    return data;
  }
  const changedNotice = () => current?.inline ? 'Unsaved changes. Save draft or return to Drafts to save.' : persistent ? 'Unsaved changes. Choose Save draft or Save & close.' : 'Draft saved in this tab.';
  function selection(owner) {
    document.dispatchEvent(new CustomEvent('cmail:draft-selection', { detail: { id: owner?.draft.cloud_id || null, editing: Boolean(owner?.inline) } }));
  }
  function inlineRoute(owner, restore = false) {
    const url = new URL(location.href);
    url.searchParams.set('view', 'drafts'); url.searchParams.set('draft', owner.draft.cloud_id || 'new'); url.hash = '';
    if (owner.mailbox) url.searchParams.set('mailbox', owner.mailbox); else url.searchParams.delete('mailbox');
    if (history.state?.cmailDraft || restore) history.replaceState({ cmailDraft: true }, '', url);
    else {
      // A directly linked draft gets the same Back-to-list step as a row click.
      const list = new URL(url); list.searchParams.delete('draft');
      history.replaceState(null, '', list); history.pushState({ cmailDraft: true }, '', url);
    }
    owner.route = url.href;
  }
  function clearInlineRoute() {
    const url = new URL(location.href); url.searchParams.delete('draft');
    history.replaceState(null, '', url);
  }
  function draftValue(value) {
    return { kind: value.kind || 'compose', sender: value.sender, to: value.to, pending: value.pending || '', subject: value.subject,
      cc: value.cc || [], bcc: value.bcc || [],
      attachments: value.attachments || [],
      in_reply_to: value.in_reply_to || '', references: value.references || [],
      text: value.text, signature: value.signature || '', ...(value.reply_id ? { reply_id: value.reply_id } : {}) };
  }
  function cloudDraft(result) {
    return { ...result.draft, cloud_id: result.id, revision: result.revision, saved_at: result.updated_at, dirty: false,
      locked: result.state === 'sending', request_id: result.request_id, error: result.error || '' };
  }
  async function saveCloud(owner = current) {
    if (!owner || owner.sent || !persistent) return true;
    if (owner.savingPromise) return owner.savingPromise;
    if (owner.draft.locked) {
      if (!owner.draft.cloud_id) { openingNotice.textContent = 'Confirm the earlier send before starting another draft.'; openingNotice.hidden = false; }
      return Boolean(owner.draft.cloud_id);
    }
    if (owner.draft.cloud_id && !owner.draft.dirty && !owner.draft.pending_save) return true;
    owner.draft.cloud_id ||= 'draft_' + crypto.randomUUID();
    owner.draft.pending_save ||= { id: owner.draft.cloud_id, revision: owner.draft.revision || 0, save_id: crypto.randomUUID(), draft: draftValue(owner.draft) };
    saving = true; save(); if (current === owner) { controls(); status('Saving draft…'); }
    owner.savingPromise = (async () => {
      try {
        const result = await api('drafts/save', owner.draft.pending_save);
        if (!result.draft) throw Object.assign(new Error(errors.draft_closed), { code: 'draft_closed' });
        Object.assign(owner.draft, cloudDraft(result)); delete owner.draft.pending_save; owner.conflict = false;
        if (draft === owner.draft) save();
        if (owner.draft.source_reply_local) {
          try { sessionStorage.removeItem('cmail.reply.' + owner.draft.reply_id); } catch {}
          document.dispatchEvent(new CustomEvent('cmail:reply-draft-saved', { detail: { id: owner.draft.reply_id } }));
          delete owner.draft.source_reply_local;
        }
        if (current === owner) {
          signaturePreview(owner);
          renderAttachments(owner);
          status('Saved to Drafts.'); owner.keepLocal.hidden = true;
          if (owner.inline) { if (!owner.closing) inlineRoute(owner, true); selection(owner); }
        }
        document.dispatchEvent(new CustomEvent('cmail:drafts-changed')); return true;
      } catch (error) {
        owner.conflict = ['draft_conflict', 'draft_changed', 'draft_closed'].includes(error.code);
        if (current === owner) { status(error.message, true); owner.keepLocal.hidden = false; }
        else { openingNotice.textContent = error.message; openingNotice.hidden = false; }
        return false;
      } finally { saving = false; owner.savingPromise = null; if (current === owner) controls(); updateTrigger(); }
    })();
    return owner.savingPromise;
  }
  function signaturePreview(owner) {
    const target = owner.dialog.querySelector('.cmail-signature-preview');
    const logo = owner.draft.signature_logo;
    const parts = splitSignature(owner.text.value, owner.draft.signature);
    target.replaceChildren(); target.hidden = !logo || !parts;
    if (target.hidden) return;
    const label = document.createElement('span'); label.className = 'cmail-signature-label'; label.textContent = 'Signature preview';
    const card = document.createElement('div'); card.className = 'cmail-signature-card';
    const image = document.createElement('img'); image.src = logo.url; image.alt = logo.alt; image.width = image.height = 96;
    const words = document.createElement('div'); words.textContent = parts.signature;
    card.append(image, words); target.append(label, card);
  }
  function renderAttachments(owner) {
    if (!owner.files) return;
    owner.files.replaceChildren();
    for (const file of owner.draft.attachments || []) {
      const row = document.createElement('li'), link = document.createElement('a'), size = document.createElement('span'), remove = document.createElement('button');
      link.href = '/cmail/api/drafts/attachments/' + file.id; link.textContent = file.filename; link.download = file.filename;
      size.textContent = file.size < 1024 ? file.size + ' B' : file.size < 1024 * 1024 ? (file.size / 1024).toFixed(1) + ' KB' : (file.size / (1024 * 1024)).toFixed(1) + ' MB';
      remove.type = 'button'; remove.textContent = 'Remove'; remove.setAttribute('aria-label', 'Remove attachment ' + file.filename);
      remove.addEventListener('click', async () => {
        if (owner.adding || saving || busy || owner.draft.locked || owner.sent || owner.draft.pending_save) return;
        owner.draft.attachments = owner.draft.attachments.filter((item) => item.id !== file.id);
        save(true); renderAttachments(owner); controls(); await saveCloud(owner); owner.attach.focus();
      });
      row.append(link, size, remove); owner.files.append(row);
    }
    const files = owner.draft.attachments || [], total = files.reduce((sum, file) => sum + file.size, 0);
    owner.fileHint.textContent = (files.length ? files.length + (files.length === 1 ? ' file · ' : ' files · ') + (total / (1024 * 1024)).toFixed(1) + ' MB attached. ' : '') + 'Up to 20 MB total. Drop files here or paste an image. Some executable file types are blocked by the email provider.';
  }
  async function addFiles(owner, selected) {
    if (!persistent || current !== owner || owner.adding || saving || busy || owner.draft.locked || owner.sent || owner.draft.pending_save) return;
    const files = [...selected]; if (!files.length) return;
    const limits = config.attachments;
    let count = (owner.draft.attachments || []).length, total = (owner.draft.attachments || []).reduce((sum, file) => sum + file.size, 0);
    for (const file of files) {
      const extension = file.name.trim().replace(/[. ]+$/, '').split('.').pop().toLowerCase();
      const error = !file.size ? 'attachment_empty' : limits.blocked_extensions.includes(extension) ? 'attachment_type_blocked' : ++count > limits.max_count ? 'too_many_attachments' : (total += file.size) > limits.max_bytes ? 'attachments_too_large' : null;
      if (error) { status(file.name + ': ' + errors[error], true); return; }
    }
    owner.adding = true; controls();
    try {
      if (!await saveCloud(owner)) return;
      for (const [index, file] of files.entries()) {
        status('Uploading ' + (index + 1) + ' of ' + files.length + ': ' + file.name + '…');
        const id = crypto.randomUUID(); let data;
        for (let attempt = 0; attempt < 2; attempt++) {
          try {
            const response = await fetch('/cmail/api/drafts/attachments/' + id, { method: 'POST', credentials: 'same-origin',
              headers: { 'Content-Type': 'application/octet-stream', 'X-Cmail-Action': 'draft', 'X-Cmail-Filename': encodeURIComponent(file.name), 'X-Cmail-Content-Type': file.type || 'application/octet-stream' }, body: file });
            if (response.redirected || !response.headers.get('Content-Type')?.includes('application/json')) throw new Error('Your sign-in expired. Sign in and choose the file again.');
            data = await response.json();
            if (!response.ok) throw Object.assign(new Error(errors[data.error] || errors.attachment_unavailable), { status: response.status });
            break;
          } catch (error) { if (attempt || error.status && error.status < 500) throw error; }
        }
        owner.draft.attachments ||= []; owner.draft.attachments.push(data.attachment);
        save(true); renderAttachments(owner);
        if (!await saveCloud(owner)) return;
      }
      status('Attachments saved to Drafts.');
    } catch (error) { status(error.message || errors.attachment_unavailable, true); }
    finally { owner.adding = false; if (current === owner) { controls(); owner.attach.focus(); } }
  }
  function loadContacts() {
    if (contactsPromise || contactsAt > Date.now() - 300000) return contactsPromise;
    contactsError = false;
    contactsPromise = (async () => {
      for (let attempt = 0; attempt < 3; attempt++) {
        const collected = new Map(); let cursor = '', revision;
        try {
          do {
            const page = await api('contacts?' + new URLSearchParams({ ...(cursor ? { cursor } : {}), ...(revision !== undefined ? { revision: String(revision) } : {}) }));
            if (signedOut) return;
            revision = page.revision;
            for (const contact of page.items) collected.set(contact.address, contact);
            contacts = [...collected.values()]; cursor = page.next_cursor || '';
            if (!cursor) contactsAt = Date.now();
            suggest();
          } while (cursor);
          return;
        } catch (error) { if (error.code !== 'contacts_changed' || attempt === 2) throw error; }
      }
    })().catch(() => { contactsError = true; }).finally(() => { contactsPromise = null; suggest(); });
    return contactsPromise;
  }
  function hideSuggestions() {
    if (!current) return;
    current.list.replaceChildren(); current.list.hidden = true; current.unique.hidden = true;
    current.to.setAttribute('aria-expanded', 'false'); current.to.removeAttribute('aria-activedescendant'); current.options = []; current.active = -1;
  }
  function suggest() {
    if (!current) return;
    hideSuggestions();
    const owner = current;
    owner.hint.textContent = owner.draft.kind === 'reply' ? 'Replying to the sender in this conversation.' : contactsError ? 'Suggestions unavailable. You can enter a full email address.' : contactsPromise && !contactsAt ? 'Loading addresses from your mail…' : 'Type a name or email address. Use Tab or Enter to add it.';
    if (document.activeElement !== owner.to || owner.draft.locked || owner.sent) return;
    const result = findMatches(contacts, owner.to.value, owner.draft.to);
    owner.options = result.items;
    if (!result.items.length) return;
    if (result.total === 1 && contactsAt) {
      const contact = result.items[0];
      owner.unique.textContent = (contact.name ? contact.name + ' · ' : '') + contact.address + ' — Tab to complete';
      owner.unique.hidden = false;
      owner.to.setAttribute('aria-describedby', 'composeUnique composeRecipientHint');
      return;
    }
    owner.to.setAttribute('aria-describedby', 'composeRecipientHint');
    owner.list.hidden = false; owner.to.setAttribute('aria-expanded', 'true');
    for (const [index, contact] of result.items.entries()) {
      const option = document.createElement('div'); option.id = 'composeOption' + index;
      option.setAttribute('role', 'option'); option.setAttribute('aria-selected', 'false'); option.className = 'cmail-contact-option';
      const name = document.createElement('strong'), address = document.createElement('span');
      name.textContent = contact.name || contact.address; address.textContent = contact.address;
      option.append(name, address); option.addEventListener('mousedown', (event) => event.preventDefault());
      option.addEventListener('click', () => add(contact.address)); owner.list.append(option);
    }
  }
  function tokens() {
    const owner = current; if (!owner) return;
    owner.tokens.replaceChildren();
    for (const address of owner.draft.to) {
      const token = document.createElement('span'); token.className = 'cmail-recipient'; token.textContent = address;
      const remove = document.createElement('button'); remove.type = 'button'; remove.textContent = '×'; remove.setAttribute('aria-label', 'Remove ' + address);
      remove.disabled = busy || owner.draft.locked || owner.sent;
      remove.addEventListener('click', () => { owner.draft.to = owner.draft.to.filter((value) => value !== address); tokens(); save(true); controls(); owner.to.focus(); suggest(); });
      token.append(remove); owner.tokens.append(token);
    }
  }
  function add(value) {
    const owner = current; if (!owner || owner.draft.locked || busy || owner.sent) return false;
    const values = value.split(/[,;\n]+/).map((part) => part.trim().replace(/^.*<([^<>]+)>$/, '$1').toLowerCase()).filter(Boolean);
    if (!values.length || values.some((address) => !email.test(address) || address.length > 254)) return false;
    const combined = [...new Set([...owner.draft.to, ...values])];
    if (combined.length > 10) { status(errors.invalid_recipients, true); return false; }
    owner.draft.to = combined; owner.draft.pending = ''; owner.to.value = '';
    tokens(); hideSuggestions(); save(true); controls(); status(changedNotice()); return true;
  }
  function controls() {
    if (!current) return;
    const owner = current, locked = busy || saving || owner.adding || owner.draft.locked || owner.sent || Boolean(owner.draft.pending_save);
    for (const input of [owner.from, owner.to, owner.cc, owner.bcc, owner.subject, owner.text]) input.disabled = Boolean(locked);
    owner.cc.parentElement.hidden = owner.bcc.parentElement.hidden = owner.draft.kind === 'reply';
    for (const button of owner.tokens.querySelectorAll('button')) button.disabled = Boolean(locked);
    if (owner.draft.kind === 'reply') {
      owner.from.disabled = true; owner.to.disabled = true; owner.subject.disabled = true;
      for (const button of owner.tokens.querySelectorAll('button')) button.disabled = true;
    }
    owner.send.hidden = Boolean(owner.sent); owner.discard.hidden = Boolean(owner.sent);
    owner.send.textContent = busy ? 'Sending…' : owner.draft.locked ? 'Retry confirmation' : 'Send';
    const hasRecipients = owner.draft.to.length > 0 || Boolean(owner.to.value.trim());
    const hasContent = owner.draft.attachments?.length || owner.text.value.trim() && owner.text.value.trim() !== owner.draft.signature.trim();
    owner.send.disabled = busy || saving || owner.adding || owner.conflict || Boolean(owner.draft.pending_save) || owner.sent || !hasRecipients || !hasContent;
    owner.attach.disabled = owner.fileInput.disabled = Boolean(locked);
    for (const button of owner.files.querySelectorAll('button')) button.disabled = Boolean(locked);
    owner.discard.disabled = Boolean(locked); owner.cancel.textContent = owner.inline ? '← Drafts' : owner.sent ? 'Done' : persistent ? 'Save & close' : 'Cancel';
    owner.cancel.title = owner.inline && !owner.sent ? 'Save and return to Drafts' : '';
    if (persistent) {
      owner.saveButton.hidden = Boolean(owner.sent); owner.saveButton.disabled = busy || saving || owner.adding || owner.draft.locked || owner.conflict;
      owner.saveButton.textContent = saving ? 'Saving…' : 'Save draft'; owner.copyButton.hidden = !owner.conflict;
      owner.cancel.disabled = busy || saving || owner.adding;
    }
    if (locked) hideSuggestions(); updateTrigger();
  }
  async function close(goBack = true, keepLocal = false, replacing = false) {
    const owner = current; if (!owner) return true;
    if (owner.closing || busy || owner.adding) {
      if (owner.inline && location.href !== owner.route) history.pushState({ cmailDraft: true }, '', owner.route);
      return false;
    }
    owner.closing = true;
    if (persistent && !owner.sent && !keepLocal && !(owner.draft.locked && !owner.draft.cloud_id)) {
      // A popstate may already have changed the address bar. Save must not
      // replace that destination while the outgoing editor is closing.
      if ((meaningful() || owner.draft.cloud_id) && !await saveCloud(owner)) {
        if (owner.inline && location.href !== owner.route) history.pushState({ cmailDraft: true }, '', owner.route);
        else if (!owner.inline && !goBack && !history.state?.cmailCompose) history.pushState({ ...history.state, cmailCompose: true }, '', location.href);
        owner.closing = false;
        return false;
      }
      if (draft === owner.draft) clear();
    }
    if (!owner.sent) save();
    owner.dialog.close?.(); owner.dialog.remove(); current = null;
    if (owner.inline) {
      selection(null);
      if (!replacing) {
        if (goBack && history.state?.cmailDraft) history.back(); else clearInlineRoute();
      }
    } else if (goBack && history.state?.cmailCompose) history.back();
    if (!replacing) trigger.focus();
    document.dispatchEvent(new CustomEvent('cmail:composer-closed'));
    return true;
  }
  async function applyReceipt(result, owner) {
    if (result.state === 'sent') {
      owner.sent = true; if (draft === owner.draft) clear(); contactsAt = 0;
      if (current === owner) {
        status('Sent. Your email is saved in Sent.');
        owner.sentLink.href = '/cmail/?mailbox=' + encodeURIComponent(owner.draft.sender) + '&view=sent#' + result.archive_id;
        owner.sentLink.hidden = false;
      }
      if ($('mailView').value !== 'drafts') $('refresh')?.click();
    } else {
      owner.draft.locked = !(result.state === 'unsent' || result.can_edit);
      if (!owner.draft.locked) owner.draft.request_id = null;
      owner.draft.error = result.error || '';
      if (draft === owner.draft) save();
      if (current === owner) status(errors[result.error] || '', Boolean(result.error));
    }
    if (current === owner) controls();
  }
  async function send() {
    const owner = current;
    if (!owner || busy || saving || owner.adding || owner.sent) return;
    if (!owner.draft.locked && owner.to.value.trim() && !add(owner.to.value)) { status(errors.invalid_recipients, true); owner.to.focus(); return; }
    if (!owner.draft.to.length || !owner.draft.attachments?.length && (!owner.text.value.trim() || owner.text.value.trim() === owner.draft.signature.trim())) return;
    if (persistent && !(owner.draft.locked && !owner.draft.cloud_id)) {
      if (!await saveCloud(owner)) return;
      busy = true; owner.draft.locked = true; save(); controls(); status('Sending…');
      try {
        const result = await api('drafts/' + owner.draft.cloud_id + '/send', { revision: owner.draft.revision });
        if (result.draft) Object.assign(owner.draft, cloudDraft(result));
        await applyReceipt({ state: result.state === 'editing' ? 'rejected' : result.state === 'sending' ? 'uncertain' : result.state,
          archive_id: result.archive_id, can_edit: result.state === 'editing', error: result.error }, owner);
        document.dispatchEvent(new CustomEvent('cmail:drafts-changed'));
      } catch (error) {
        owner.conflict = error.code === 'draft_conflict';
        if (owner.conflict) { owner.draft.locked = false; owner.draft.dirty = true; }
        else if (error.status === 400) {
          try { const latest = await api('drafts/' + owner.draft.cloud_id); if (latest.draft) Object.assign(owner.draft, cloudDraft(latest)); } catch {}
        }
        if (draft === owner.draft) save(); if (current === owner) { status(error.message, true); owner.keepLocal.hidden = false; }
      } finally { busy = false; if (current === owner) controls(); updateTrigger(); }
      return;
    }
    owner.draft.request_id ||= crypto.randomUUID(); owner.draft.locked = true;
    busy = true; save(); controls(); status('Sending…');
    try { await applyReceipt(await api('compose', { ...draftValue(owner.draft), request_id: owner.draft.request_id }), owner); }
    catch (error) {
      // Validation failures happen before provider submission; every other
      // failure retains the exact payload and request ID for safe confirmation.
      if (error.status === 400) { owner.draft.locked = false; owner.draft.request_id = null; }
      owner.draft.error = error.code || 'send_unconfirmed';
      if (draft === owner.draft) save();
      if (current === owner) status(error.message, true);
    } finally { busy = false; if (current === owner) controls(); updateTrigger(); }
  }
  async function open(options = {}) {
    if (opening || busy || saving) return;
    if (current && (!current.inline || !options.fresh && (!options.id || options.id === current.draft.cloud_id))) return;
    opening = true; openingNotice.hidden = true; updateTrigger();
    try {
      if (current && !await close(false, false, true)) return;
      config ||= await api('compose');
      if (!config.identities.length) throw new Error('No sending identities are configured.');
      const identity = config.identities.find((item) => item.address === $('mailbox').value) || config.identities[0];
      if (persistent && (options.fresh || options.id && options.id !== draft?.cloud_id || options.reply_id && options.reply_id !== draft?.reply_id)) {
        if (draft && meaningful() && !await saveCloud({ draft })) return;
        clear();
      }
      const cloudId = options.id || draft?.cloud_id;
      if (persistent && cloudId && !draft?.dirty && !draft?.pending_save) {
        const item = await api('drafts/' + cloudId);
        if (!item.draft) { clear(); throw new Error(item.state === 'sent' ? 'This draft has already been sent. You can find it in Sent.' : 'This draft was discarded.'); }
        draft = cloudDraft(item);
      }
      if (persistent && options.reply_id && !draft) {
        const item = await api('drafts/reply/' + options.reply_id);
        if (item.draft && !options.legacy?.text) draft = cloudDraft(item);
        else {
          const context = item.context || item.draft;
          draft = { kind: 'reply', reply_id: options.reply_id, sender: context.sender, to: context.to, subject: context.subject,
            signature: context.signature, signature_logo: context.signature_logo, text: options.legacy?.text || (context.signature ? '\n\n' + context.signature : ''), pending: '', dirty: true, source_reply_local: Boolean(options.legacy) };
        }
      }
      draft ||= { kind: 'compose', dirty: persistent, sender: identity.address, signature: identity.signature, signature_logo: identity.signature_logo, to: [], pending: '', subject: '', text: identity.signature ? '\n\n' + identity.signature : '' };
      const inline = persistent && $('mailView').value === 'drafts' && Boolean($('draftEditor'));
      const dialog = document.createElement(inline ? 'section' : 'dialog'); dialog.className = inline ? 'cmail-compose-inline' : 'cmail-compose-dialog'; dialog.setAttribute('aria-labelledby', 'composeHeading');
      dialog.innerHTML = `<form class="cmail-compose-form"><header><h2 id="composeHeading">New message</h2><button type="button" id="composeCancel">Cancel</button></header>
        <label class="cmail-compose-field">From<select id="composeFrom" aria-label="From"></select></label>
        <div class="cmail-compose-field cmail-compose-to"><label for="composeTo">To</label><div class="cmail-recipient-field"><div id="composeTokens"></div><input id="composeTo" type="text" role="combobox" aria-label="To" aria-autocomplete="list" aria-controls="composeOptions" aria-expanded="false" aria-describedby="composeRecipientHint" autocomplete="off" autocapitalize="none" spellcheck="false" placeholder="Name or email address"><div id="composeOptions" role="listbox" aria-label="Matching contacts" hidden></div><button id="composeUnique" type="button" hidden></button></div></div>
        <p id="composeRecipientHint" class="cmail-compose-hint"></p>
        <label class="cmail-compose-field">Cc<input id="composeCc" aria-label="Cc" autocomplete="off" placeholder="Email addresses, separated by commas"></label>
        <label class="cmail-compose-field">Bcc<input id="composeBcc" aria-label="Bcc" autocomplete="off" placeholder="Email addresses, separated by commas"></label>
        <label class="cmail-compose-field">Subject<input id="composeSubject" maxlength="1000" autocomplete="off"></label>
        <label class="cmail-compose-body"><span class="cmail-compose-sr">Message</span><textarea id="composeText" aria-label="Message" maxlength="100000" placeholder="Write your message…"></textarea></label>
        <section class="cmail-signature-preview" aria-label="Signature preview" hidden></section>
        <section class="cmail-attachments" aria-label="Attachments"><div><button type="button" id="composeAttach">Attach files</button><input type="file" id="composeFiles" aria-label="Choose attachments" multiple hidden></div><ul id="composeAttachments"></ul><p id="composeAttachmentHint"></p></section>
        <footer><button id="composeSend" type="button" class="cmail-reply-send">Send</button><button id="composeSave" type="button" hidden>Save draft</button><span id="composeStatus" class="cmail-reply-status" role="status"></span><a id="composeSentLink" hidden>View in Sent</a><button id="composeCopy" type="button" hidden>Save a copy</button><button id="composeKeepLocal" type="button" hidden>Close, keep local copy</button><button id="composeDiscard" type="button">Discard draft</button></footer></form>`;
      (inline ? $('draftEditor') : document.body).append(dialog);
      const owner = current = { dialog, inline, mailbox: $('mailbox').value, draft, active: -1, options: [], from: $('composeFrom'), to: $('composeTo'), tokens: $('composeTokens'), list: $('composeOptions'), unique: $('composeUnique'), hint: $('composeRecipientHint'), subject: $('composeSubject'), text: $('composeText'), send: $('composeSend'), cancel: $('composeCancel'), discard: $('composeDiscard'), status: $('composeStatus'), sentLink: $('composeSentLink') };
      Object.assign(owner, { cc: $('composeCc'), bcc: $('composeBcc'), saveButton: $('composeSave'), copyButton: $('composeCopy'), keepLocal: $('composeKeepLocal') });
      Object.assign(owner, { attach: $('composeAttach'), fileInput: $('composeFiles'), files: $('composeAttachments'), fileHint: $('composeAttachmentHint') });
      owner.attach.closest('section').hidden = !persistent || !config.attachments;
      owner.attach.addEventListener('click', () => owner.fileInput.click());
      owner.fileInput.addEventListener('change', () => { const files = [...owner.fileInput.files]; owner.fileInput.value = ''; addFiles(owner, files); });
      dialog.addEventListener('dragover', (event) => { if (event.dataTransfer?.types.includes('Files')) { event.preventDefault(); dialog.classList.add('cmail-file-drag'); } });
      dialog.addEventListener('dragleave', (event) => { if (!dialog.contains(event.relatedTarget)) dialog.classList.remove('cmail-file-drag'); });
      dialog.addEventListener('drop', (event) => { dialog.classList.remove('cmail-file-drag'); if (event.dataTransfer?.files.length) { event.preventDefault(); addFiles(owner, event.dataTransfer.files); } });
      dialog.addEventListener('paste', (event) => { if (event.clipboardData?.files.length) { event.preventDefault(); addFiles(owner, event.clipboardData.files); } });
      renderAttachments(owner);
      $('composeHeading').textContent = draft.kind === 'reply' ? 'Reply draft' : persistent ? draft.cloud_id ? 'Draft' : 'New draft' : 'New message';
      dialog.setAttribute('aria-label', draft.kind === 'reply' ? 'Reply draft' : persistent ? 'Email draft' : 'New message');
      for (const item of config.identities) { const option = document.createElement('option'); option.value = item.address; option.textContent = item.from; owner.from.append(option); }
      owner.from.value = draft.sender; owner.to.value = draft.pending || ''; owner.subject.value = draft.subject; owner.text.value = draft.text;
      for (const field of ['cc', 'bcc']) {
        owner[field].value = (draft[field] || []).join(', ');
        owner[field].addEventListener('input', () => {
          owner.draft[field] = owner[field].value.split(/[,;]/).map((v) => v.trim()).filter(Boolean);
          save(true); controls(); status(changedNotice());
        });
      }
      owner.from.addEventListener('change', () => {
        const identity = config.identities.find((item) => item.address === owner.from.value);
        const old = owner.draft.signature;
        if (old && owner.text.value.endsWith(old)) owner.text.value = owner.text.value.slice(0, -old.length) + identity.signature;
        else if (!owner.text.value.trim()) owner.text.value = identity.signature ? '\n\n' + identity.signature : '';
        owner.draft.sender = identity.address; owner.draft.signature = identity.signature; owner.draft.signature_logo = identity.signature_logo; owner.draft.text = owner.text.value; signaturePreview(owner); save(true); controls(); status(changedNotice());
      });
      owner.to.addEventListener('input', () => { owner.draft.pending = owner.to.value; save(true); controls(); status(changedNotice()); suggest(); });
      owner.to.addEventListener('focus', suggest);
      owner.to.addEventListener('blur', () => { if (owner.to.value.trim()) add(owner.to.value); hideSuggestions(); });
      owner.to.addEventListener('keydown', (event) => {
        if (['ArrowDown', 'ArrowUp'].includes(event.key) && owner.options.length && !owner.list.hidden) {
          event.preventDefault(); owner.active = (owner.active + (event.key === 'ArrowDown' ? 1 : -1) + owner.options.length) % owner.options.length;
          for (const [index, option] of [...owner.list.children].entries()) option.setAttribute('aria-selected', String(index === owner.active));
          owner.to.setAttribute('aria-activedescendant', 'composeOption' + owner.active); return;
        }
        if (event.key === 'Escape' && (!owner.list.hidden || !owner.unique.hidden)) { event.preventDefault(); event.stopPropagation(); hideSuggestions(); return; }
        if (event.key === 'Tab' || event.key === 'Enter' || event.key === ',' || event.key === ';') {
          const candidate = owner.active >= 0 ? owner.options[owner.active] : owner.options.length === 1 ? owner.options[0] : null;
          if (candidate && (event.key === 'Tab' || event.key === 'Enter')) { event.preventDefault(); add(candidate.address); }
          else if (event.key === 'Enter' || event.key === ',' || event.key === ';') { event.preventDefault(); if (!add(owner.to.value)) status(errors.invalid_recipients, true); }
          else if (owner.to.value.trim()) add(owner.to.value);
        }
      });
      owner.unique.addEventListener('mousedown', (event) => event.preventDefault());
      owner.unique.addEventListener('click', () => { if (owner.options[0]) { add(owner.options[0].address); owner.to.focus(); } });
      for (const [input, field] of [[owner.subject, 'subject'], [owner.text, 'text']]) input.addEventListener('input', () => { owner.draft[field] = input.value; owner.draft.error = ''; signaturePreview(owner); save(true); controls(); status(changedNotice()); });
      owner.subject.addEventListener('keydown', (event) => { if (event.key === 'Enter') { event.preventDefault(); owner.text.focus(); } });
      dialog.querySelector('form').addEventListener('submit', (event) => event.preventDefault());
      owner.send.addEventListener('click', send); owner.cancel.addEventListener('click', () => close());
      owner.saveButton.addEventListener('click', () => saveCloud(owner));
      owner.copyButton.addEventListener('click', async () => {
        Object.assign(owner.draft, { cloud_id: null, revision: 0, dirty: true, locked: false, request_id: null }); delete owner.draft.pending_save;
        owner.conflict = false; save(); await saveCloud(owner);
      });
      owner.keepLocal.addEventListener('click', () => close(true, true));
      owner.discard.addEventListener('click', async () => {
        if (persistent && owner.draft.cloud_id) {
          try { await api('drafts/' + owner.draft.cloud_id + '/discard', { revision: owner.draft.revision }); }
          catch (error) { status(error.message, true); return; }
        }
        owner.sent = true; clear(); close(); document.dispatchEvent(new CustomEvent('cmail:drafts-changed'));
      });
      dialog.addEventListener('cancel', (event) => { event.preventDefault(); if (!owner.list.hidden || !owner.unique.hidden) hideSuggestions(); else close(); });
      tokens();
      signaturePreview(owner);
      if (inline) { inlineRoute(owner, options.restore); selection(owner); }
      else {
        dialog.showModal();
        if (!history.state?.cmailCompose) history.pushState({ ...history.state, cmailCompose: true }, '', location.href);
      }
      controls(); status(errors[draft.error] || (persistent ? draft.cloud_id && !draft.dirty ? 'Saved to Drafts.' : 'Choose Save draft to keep this email without sending.' : '')); (draft.cloud_id || draft.kind === 'reply' ? owner.text : owner.to).focus(); save();
      if (draft.kind !== 'reply') loadContacts(); else suggest();
      if (draft.locked && (draft.request_id || draft.cloud_id)) {
        busy = true; controls();
        try {
          if (persistent && draft.cloud_id) {
            const result = await api('drafts/' + draft.cloud_id);
            if (result.draft) Object.assign(owner.draft, cloudDraft(result));
            await applyReceipt({ state: result.state === 'editing' ? 'unsent' : result.state === 'sending' ? 'uncertain' : result.state, archive_id: result.archive_id, error: result.error }, owner);
          } else await applyReceipt(await api('replies/' + draft.request_id), owner);
        }
        catch (error) { if (current === owner) status(error.message, true); }
        finally { busy = false; if (current === owner) controls(); }
      }
    } catch (error) {
      if (current) status(error.message, true);
      else {
        if (history.state?.cmailDraft) clearInlineRoute();
        openingNotice.textContent = error.message; openingNotice.hidden = false;
      }
    } finally { opening = false; updateTrigger(); }
  }
  trigger.addEventListener('click', () => open());
  if (persistent) {
    document.addEventListener('cmail:new-draft', () => open({ fresh: true }));
    document.addEventListener('cmail:open-draft', (event) => open({ id: event.detail.id }));
    document.addEventListener('cmail:reply-draft', (event) => open({ reply_id: event.detail.id, legacy: event.detail.legacy }));
    document.addEventListener('cmail:before-view-change', (event) => {
      if (opening) event.detail.ready = Promise.resolve(false);
      else if (current?.inline) event.detail.ready = close(false);
    });
    document.querySelector('.brand')?.addEventListener('click', (event) => {
      if (!current?.inline || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      event.preventDefault(); $('mailView').value = 'inbox'; $('mailView').dispatchEvent(new Event('change'));
    });
  }
  document.addEventListener('keydown', (event) => {
    if (event.defaultPrevented || event.key !== 'Escape' || !current?.inline) return;
    event.preventDefault();
    if (!current.list.hidden || !current.unique.hidden) hideSuggestions(); else close();
  });
  window.addEventListener('popstate', async () => {
    if (current && !current.inline) { if (!history.state?.cmailCompose) await close(false); return; }
    const destination = new URL(location.href), id = destination.searchParams.get('draft');
    if (current?.inline) {
      if (destination.href === current.route) return;
      if (!await close(false, false, true)) return;
    }
    if (!persistent) return;
    document.dispatchEvent(new CustomEvent('cmail:restore-view'));
    if (destination.searchParams.get('view') === 'drafts' && (id === 'new' || /^draft_[a-f0-9-]{36}$/.test(id || ''))) await open({ ...(id === 'new' ? { fresh: true } : { id }), restore: true });
  });
  window.addEventListener('pagehide', () => { if (draft) save(); });
  window.addEventListener('beforeunload', (event) => { if (current?.adding) { event.preventDefault(); event.returnValue = ''; } });
  $('signOut').addEventListener('click', () => { signedOut = true; clear(); contacts = []; config = null; if (current) { current.sent = true; close(false); } }, true);
  updateTrigger();
  if (persistent) {
    const linked = new URLSearchParams(location.search).get('draft');
    if (/^draft_[a-f0-9-]{36}$/.test(linked || '') || linked === 'new') {
      const openLinked = () => open(linked === 'new' ? { fresh: true } : { id: linked });
      if ($('draftEditor')) openLinked(); else document.addEventListener('cmail:drafts-ready', openLinked, { once: true });
    }
  }
}

export const composeScript = '(' + composeApplication.toString() + ')(' + matchContacts.toString() + ',' + splitSignature.toString() + ');';
export const composeStyles = `
.cmail-compose-trigger{white-space:nowrap}
.cmail-compose-dialog{box-sizing:border-box;width:min(760px,calc(100vw - 24px));max-height:calc(100dvh - 28px);margin:auto;padding:0;border:1px solid #526445;border-radius:12px;background:var(--panel2);color:var(--text);box-shadow:0 16px 70px #0008;overflow:auto}
.cmail-compose-dialog::backdrop{background:#0008}
.cmail-compose-form{padding:22px;display:flex;flex-direction:column;gap:12px}
.cmail-compose-form header{display:flex;justify-content:space-between;align-items:center;gap:16px}
.cmail-compose-form h2{font-size:18px;margin:0}
.cmail-compose-form button,.cmail-compose-form a,.cmail-compose-form select{cursor:pointer}
.cmail-compose-form button:disabled,.cmail-compose-form select:disabled{cursor:not-allowed;opacity:.5}
.cmail-compose-form button:not(.cmail-reply-send){font:inherit;font-size:12px;background:transparent;color:var(--muted);border:0;padding:7px;border-radius:5px}
.cmail-compose-form button:hover:not(:disabled){filter:brightness(1.2)}
.cmail-compose-field{display:flex;gap:14px;align-items:center;font-size:13px;min-height:38px}
.cmail-compose-field>label{min-width:28px}
.cmail-compose-field input,.cmail-compose-field select{flex:1;min-width:0;font:inherit;color:var(--text);background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:10px}
.cmail-recipient-field{position:relative;flex:1;min-width:0}
#composeTo{width:100%;box-sizing:border-box}
#composeTokens{display:flex;flex-wrap:wrap;gap:5px}
.cmail-recipient{display:inline-flex;align-items:center;gap:4px;max-width:100%;overflow-wrap:anywhere;border-radius:6px;border:1px solid var(--line);background:var(--bg);font-size:12px;padding-left:8px;margin-bottom:5px}
#composeOptions{position:absolute;z-index:2;left:0;right:0;top:100%;margin-top:3px;max-height:270px;overflow:auto;background:var(--panel2);border:1px solid #526445;border-radius:8px;box-shadow:0 8px 25px #0006}
.cmail-contact-option{display:flex;flex-direction:column;gap:3px;padding:10px 12px;cursor:pointer;overflow-wrap:anywhere}
.cmail-contact-option span{font-size:12px;color:var(--muted)}
.cmail-contact-option:hover,.cmail-contact-option[aria-selected=true]{background:#34432b}
.cmail-compose-form #composeUnique{width:100%;text-align:left;color:var(--accent);padding:8px 2px;overflow-wrap:anywhere}
.cmail-compose-hint{margin:0 0 0 42px;color:var(--muted);font-size:11px;line-height:1.5}
.cmail-compose-body textarea{box-sizing:border-box;width:100%;min-height:240px;height:30vh;resize:vertical;padding:14px;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--text);font:inherit;font-size:14px;line-height:1.6}
.cmail-signature-preview{padding:2px 0 8px}.cmail-signature-preview[hidden]{display:none}
.cmail-signature-label{display:block;color:var(--muted);font-size:11px;margin-bottom:8px}
.cmail-signature-card{display:flex;align-items:center;gap:18px;padding:18px;background:#fffef6;border-radius:6px;color:#004b36;font:13px/1.6 Arial,Helvetica,sans-serif}
.cmail-signature-card img{display:block;width:96px;height:96px;flex-shrink:0}.cmail-signature-card div{border-left:2px solid #cf4d1c;padding-left:18px;white-space:pre-line;overflow-wrap:anywhere;min-width:0}
.cmail-compose-form input:focus,.cmail-compose-form textarea:focus,.cmail-compose-form select:focus{outline:2px solid var(--accent);outline-offset:1px}
.cmail-compose-form footer{display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:12px}
.cmail-attachments{border:1px dashed var(--line);border-radius:7px;padding:10px;box-sizing:border-box}
.cmail-attachments[hidden]{display:none}.cmail-compose-form #composeAttach{border:1px solid #526445;color:var(--accent);padding:7px 12px}
.cmail-attachments ul{list-style:none;padding:0;margin:8px 0}.cmail-attachments ul:empty{display:none}
.cmail-attachments li{display:flex;align-items:center;gap:10px;padding:5px 0;font-size:12px;min-width:0}
.cmail-attachments li a{flex:1;min-width:0;overflow-wrap:anywhere}.cmail-attachments li span{white-space:nowrap;color:var(--muted)}
.cmail-attachments p{margin:8px 0 0;font-size:11px;color:var(--muted);line-height:1.5}.cmail-file-drag .cmail-attachments{border-color:var(--accent);background:#34432b}
#composeDiscard{margin-left:auto}.cmail-compose-form a{color:var(--accent)}
.cmail-compose-sr{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
.cmail-compose-dialog [hidden]{display:none!important}
@media(max-width:700px){.cmail-compose-form{padding:14px}.cmail-compose-dialog{width:calc(100vw - 12px);max-height:calc(100dvh - 12px)}.cmail-compose-body textarea{min-height:180px}.topbar .cmail-compose-trigger{padding:7px 10px}}
`;
