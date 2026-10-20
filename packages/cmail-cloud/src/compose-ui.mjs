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

function composeApplication(findMatches) {
  const $ = (id) => document.getElementById(id);
  const key = 'cmail.compose.draft.v1';
  const email = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}$/i;
  let draft = null, current = null, opening = false, busy = false, config = null;
  let contacts = [], contactsAt = 0, contactsPromise = null, contactsError = false, signedOut = false;
  const trigger = document.createElement('button');
  trigger.type = 'button'; trigger.id = 'composeButton'; trigger.className = 'cmail-reply-trigger cmail-compose-trigger'; trigger.textContent = 'Compose';
  $('mailbox').before(trigger);
  const openingNotice = document.createElement('span'); openingNotice.className = 'cmail-reply-status'; openingNotice.setAttribute('role', 'status'); openingNotice.hidden = true; trigger.after(openingNotice);
  try { draft = JSON.parse(sessionStorage.getItem(key) || 'null'); } catch {}
  const errors = {
    invalid_recipients: 'Enter up to 10 complete email addresses.', invalid_subject: 'Check the subject and try again.',
    invalid_reply: 'Write your message before sending.', empty_reply: 'Write your message above the signature.',
    sender_not_configured: 'Choose a configured From address.', reply_changed: 'The original send is being confirmed. Retry it before editing.',
    confirmation_expired: 'This earlier send needs a status check before it can be retried. Your draft is saved.',
    send_unconfirmed: 'The send could not be confirmed. Retry confirmation to check it safely.',
    send_rejected: 'The mail service did not accept this message. Your draft is saved and can be edited.',
    send_rate_limited: 'The mail service is busy. Your draft is saved; try again shortly.',
    archive_pending: 'Your email was sent. Retry confirmation to finish saving its Sent copy.',
  };
  const meaningful = () => draft && (draft.to?.length || draft.pending || draft.subject || draft.text?.trim() !== draft.signature?.trim());
  function updateTrigger() { trigger.textContent = opening ? 'Opening…' : meaningful() ? 'Continue draft' : 'Compose'; trigger.disabled = opening || busy; }
  function save() {
    if (signedOut || !draft) return;
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
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', 'X-Cmail-Action': 'compose' }, body: JSON.stringify(body),
    });
    if (response.redirected || !response.headers.get('Content-Type')?.includes('application/json')) throw new Error('Your sign-in expired. Refresh to sign in; your draft is saved.');
    const data = await response.json();
    if (!response.ok) throw Object.assign(new Error(errors[data.error] || 'Temporarily unavailable. Your draft is saved.'), { code: data.error, status: response.status });
    return data;
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
    owner.hint.textContent = contactsError ? 'Suggestions unavailable. You can enter a full email address.' : contactsPromise && !contactsAt ? 'Loading addresses from your mail…' : 'Type a name or email address. Use Tab or Enter to add it.';
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
      remove.addEventListener('click', () => { owner.draft.to = owner.draft.to.filter((value) => value !== address); tokens(); save(); controls(); owner.to.focus(); suggest(); });
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
    tokens(); hideSuggestions(); save(); controls(); status('Draft saved in this tab.'); return true;
  }
  function controls() {
    if (!current) return;
    const owner = current, locked = busy || owner.draft.locked || owner.sent;
    for (const input of [owner.from, owner.to, owner.subject, owner.text]) input.disabled = Boolean(locked);
    for (const button of owner.tokens.querySelectorAll('button')) button.disabled = Boolean(locked);
    owner.send.hidden = Boolean(owner.sent); owner.discard.hidden = Boolean(owner.sent);
    owner.send.textContent = busy ? 'Sending…' : owner.draft.locked ? 'Retry confirmation' : 'Send';
    const hasRecipients = owner.draft.to.length > 0 || Boolean(owner.to.value.trim());
    owner.send.disabled = busy || owner.sent || !hasRecipients || !owner.text.value.trim() || owner.text.value.trim() === owner.draft.signature.trim();
    owner.discard.disabled = Boolean(locked); owner.cancel.textContent = owner.sent ? 'Done' : 'Cancel';
    if (locked) hideSuggestions(); updateTrigger();
  }
  function close(goBack = true) {
    const owner = current; if (!owner) return;
    if (!owner.sent) save();
    owner.dialog.close(); owner.dialog.remove(); current = null; trigger.focus();
    if (goBack && history.state?.cmailCompose) history.back();
  }
  async function applyReceipt(result, owner) {
    if (result.state === 'sent') {
      owner.sent = true; if (draft === owner.draft) clear(); contactsAt = 0;
      if (current === owner) {
        status('Sent. Your email is saved in Sent.');
        owner.sentLink.href = '/cmail/?mailbox=' + encodeURIComponent(owner.draft.sender) + '&view=sent#' + result.archive_id;
        owner.sentLink.hidden = false;
      }
      $('refresh')?.click();
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
    if (!owner || busy || owner.sent) return;
    if (!owner.draft.locked && owner.to.value.trim() && !add(owner.to.value)) { status(errors.invalid_recipients, true); owner.to.focus(); return; }
    if (!owner.draft.to.length || !owner.text.value.trim() || owner.text.value.trim() === owner.draft.signature.trim()) return;
    owner.draft.request_id ||= crypto.randomUUID(); owner.draft.locked = true;
    busy = true; save(); controls(); status('Sending…');
    try { await applyReceipt(await api('compose', { request_id: owner.draft.request_id, sender: owner.draft.sender, to: owner.draft.to, subject: owner.draft.subject, text: owner.draft.text }), owner); }
    catch (error) {
      // Validation failures happen before provider submission; every other
      // failure retains the exact payload and request ID for safe confirmation.
      if (error.status === 400) { owner.draft.locked = false; owner.draft.request_id = null; }
      owner.draft.error = error.code || 'send_unconfirmed';
      if (draft === owner.draft) save();
      if (current === owner) status(error.message, true);
    } finally { busy = false; if (current === owner) controls(); updateTrigger(); }
  }
  async function open() {
    if (opening || busy || current) return;
    opening = true; openingNotice.hidden = true; updateTrigger();
    try {
      config ||= await api('compose');
      if (!config.identities.length) throw new Error('No sending identities are configured.');
      const identity = config.identities.find((item) => item.address === $('mailbox').value) || config.identities[0];
      draft ||= { sender: identity.address, signature: identity.signature, to: [], pending: '', subject: '', text: identity.signature ? '\n\n' + identity.signature : '' };
      const dialog = document.createElement('dialog'); dialog.className = 'cmail-compose-dialog'; dialog.setAttribute('aria-labelledby', 'composeHeading');
      dialog.innerHTML = `<form class="cmail-compose-form"><header><h2 id="composeHeading">New message</h2><button type="button" id="composeCancel">Cancel</button></header>
        <label class="cmail-compose-field">From<select id="composeFrom" aria-label="From"></select></label>
        <div class="cmail-compose-field cmail-compose-to"><label for="composeTo">To</label><div class="cmail-recipient-field"><div id="composeTokens"></div><input id="composeTo" type="text" role="combobox" aria-label="To" aria-autocomplete="list" aria-controls="composeOptions" aria-expanded="false" aria-describedby="composeRecipientHint" autocomplete="off" autocapitalize="none" spellcheck="false" placeholder="Name or email address"><div id="composeOptions" role="listbox" aria-label="Matching contacts" hidden></div><button id="composeUnique" type="button" hidden></button></div></div>
        <p id="composeRecipientHint" class="cmail-compose-hint"></p>
        <label class="cmail-compose-field">Subject<input id="composeSubject" maxlength="1000" autocomplete="off"></label>
        <label class="cmail-compose-body"><span class="cmail-compose-sr">Message</span><textarea id="composeText" aria-label="Message" maxlength="100000" placeholder="Write your message…"></textarea></label>
        <footer><button id="composeSend" type="button" class="cmail-reply-send">Send</button><span id="composeStatus" class="cmail-reply-status" role="status"></span><a id="composeSentLink" hidden>View in Sent</a><button id="composeDiscard" type="button">Discard draft</button></footer></form>`;
      document.body.append(dialog);
      const owner = current = { dialog, draft, active: -1, options: [], from: $('composeFrom'), to: $('composeTo'), tokens: $('composeTokens'), list: $('composeOptions'), unique: $('composeUnique'), hint: $('composeRecipientHint'), subject: $('composeSubject'), text: $('composeText'), send: $('composeSend'), cancel: $('composeCancel'), discard: $('composeDiscard'), status: $('composeStatus'), sentLink: $('composeSentLink') };
      for (const item of config.identities) { const option = document.createElement('option'); option.value = item.address; option.textContent = item.from; owner.from.append(option); }
      owner.from.value = draft.sender; owner.to.value = draft.pending || ''; owner.subject.value = draft.subject; owner.text.value = draft.text;
      owner.from.addEventListener('change', () => {
        const identity = config.identities.find((item) => item.address === owner.from.value);
        const old = owner.draft.signature;
        if (old && owner.text.value.endsWith(old)) owner.text.value = owner.text.value.slice(0, -old.length) + identity.signature;
        else if (!owner.text.value.trim()) owner.text.value = identity.signature ? '\n\n' + identity.signature : '';
        owner.draft.sender = identity.address; owner.draft.signature = identity.signature; owner.draft.text = owner.text.value; save(); controls();
      });
      owner.to.addEventListener('input', () => { owner.draft.pending = owner.to.value; save(); controls(); suggest(); });
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
      for (const [input, field] of [[owner.subject, 'subject'], [owner.text, 'text']]) input.addEventListener('input', () => { owner.draft[field] = input.value; owner.draft.error = ''; save(); controls(); status('Draft saved in this tab.'); });
      owner.subject.addEventListener('keydown', (event) => { if (event.key === 'Enter') { event.preventDefault(); owner.text.focus(); } });
      dialog.querySelector('form').addEventListener('submit', (event) => event.preventDefault());
      owner.send.addEventListener('click', send); owner.cancel.addEventListener('click', () => close());
      owner.discard.addEventListener('click', () => { owner.sent = true; clear(); close(); });
      dialog.addEventListener('cancel', (event) => { event.preventDefault(); if (!owner.list.hidden || !owner.unique.hidden) hideSuggestions(); else close(); });
      tokens(); dialog.showModal();
      if (!history.state?.cmailCompose) history.pushState({ ...history.state, cmailCompose: true }, '', location.href);
      controls(); status(errors[draft.error] || ''); owner.to.focus(); save(); loadContacts();
      if (draft.locked && draft.request_id) {
        busy = true; controls();
        try { await applyReceipt(await api('replies/' + draft.request_id), owner); }
        catch (error) { if (current === owner) status(error.message, true); }
        finally { busy = false; if (current === owner) controls(); }
      }
    } catch (error) {
      if (current) status(error.message, true);
      else { openingNotice.textContent = error.message; openingNotice.hidden = false; }
    } finally { opening = false; updateTrigger(); }
  }
  trigger.addEventListener('click', open);
  window.addEventListener('popstate', () => { if (current && !history.state?.cmailCompose) close(false); });
  window.addEventListener('pagehide', () => { if (draft) save(); });
  $('signOut').addEventListener('click', () => { signedOut = true; clear(); contacts = []; config = null; if (current) { current.sent = true; close(false); } }, true);
  updateTrigger();
}

export const composeScript = '(' + composeApplication.toString() + ')(' + matchContacts.toString() + ');';
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
.cmail-compose-form input:focus,.cmail-compose-form textarea:focus,.cmail-compose-form select:focus{outline:2px solid var(--accent);outline-offset:1px}
.cmail-compose-form footer{display:flex;align-items:center;gap:12px;flex-wrap:wrap;font-size:12px}
#composeDiscard{margin-left:auto}.cmail-compose-form a{color:var(--accent)}
.cmail-compose-sr{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}
.cmail-compose-dialog [hidden]{display:none!important}
@media(max-width:700px){.cmail-compose-form{padding:14px}.cmail-compose-dialog{width:calc(100vw - 12px);max-height:calc(100dvh - 12px)}.cmail-compose-body textarea{min-height:180px}.topbar .cmail-compose-trigger{padding:7px 10px}}
`;
