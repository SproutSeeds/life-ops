function replyApplication() {
  const $ = (id) => document.getElementById(id);
  const prefix = 'cmail.reply.';
  const memory = new Map();
  let composer = null;
  let current = null;
  let busy = false;
  let opening = false;
  let activeId = '';
  const selected = () => !$('message')?.hidden ? location.hash.match(/^#(mail_\d{16})$/)?.[1] || '' : '';
  const trigger = document.createElement('button');
  trigger.id = 'replyButton'; trigger.type = 'button'; trigger.className = 'cmail-reply-trigger'; trigger.textContent = 'Reply';
  $('back').after(trigger);
  const messages = {
    sender_not_configured: 'Sending is not configured for this inbox.', recipient_unavailable: 'This message has no address to reply to.',
    empty_reply: 'Write your reply before sending.', invalid_reply: 'Check your reply and try again.',
    reply_changed: 'This send is still being confirmed. Retry the original reply before editing it.',
    confirmation_expired: 'This earlier send needs a status check before it can be retried. Your reply is saved.',
    send_rate_limited: 'The mail service is busy. Your reply is saved; try again shortly.',
    send_rejected: 'The mail service did not accept this reply. Your text is saved. Try again or check the sending configuration.',
    send_unconfirmed: 'The send could not be confirmed. Retry to check it safely.',
    archive_pending: 'Your reply was sent. Retry to finish saving its copy.',
  };
  function getDraft(id) {
    if (memory.has(id)) return memory.get(id);
    try { const saved = JSON.parse(sessionStorage.getItem(prefix + id) || 'null'); if (saved) { memory.set(id, saved); return saved; } } catch {}
    return null;
  }
  function save() {
    if (!current) return;
    memory.set(current.id, current.draft);
    try { sessionStorage.setItem(prefix + current.id, JSON.stringify(current.draft)); } catch { status('Your draft is kept in this page. Keep this tab open until you send it.'); }
  }
  function clearDraft(id) {
    memory.delete(id);
    try { sessionStorage.removeItem(prefix + id); } catch {}
  }
  function status(text, error = false) {
    if (!composer) return;
    const line = composer.querySelector('.cmail-reply-status');
    line.textContent = text; line.classList.toggle('cmail-reply-error', error);
  }
  async function api(path, body) {
    const response = await fetch('/cmail/api/' + path, body === undefined ? { credentials: 'same-origin', cache: 'no-store' } : {
      method: 'POST', credentials: 'same-origin', headers: { 'Content-Type': 'application/json', 'X-Cmail-Action': 'reply' }, body: JSON.stringify(body),
    });
    if (response.redirected || !response.headers.get('Content-Type')?.includes('application/json')) throw new Error('Your sign-in expired. Refresh to sign in; your draft is saved.');
    const data = await response.json();
    if (!response.ok) throw new Error(messages[data.error] || 'Reply is temporarily unavailable. Your text is saved.');
    return data;
  }
  function close(goBack = true) {
    if (!composer) return;
    if (!current?.sent) save();
    composer.remove(); composer = null; current = null;
    trigger.disabled = false; trigger.focus(); update();
    if (goBack && history.state?.cmailReply) history.back();
  }
  function controls() {
    if (!composer || !current) return;
    const draft = current.draft;
    const input = composer.querySelector('textarea');
    input.readOnly = busy || Boolean(draft.locked) || Boolean(current.sent);
    const send = composer.querySelector('[type=submit]');
    send.disabled = busy || current.sent || input.value.trim() === current.context.signature.trim() || !input.value.trim();
    send.textContent = busy ? 'Sending…' : draft.locked ? 'Retry confirmation' : 'Send';
    send.hidden = Boolean(current.sent);
    composer.querySelector('.cmail-reply-cancel').textContent = current.sent ? 'Done' : 'Cancel';
  }
  async function applyReceipt(result, owner) {
    if (result.state === 'sent') {
      clearDraft(owner.id);
      owner.sent = true; owner.draft.locked = false;
      if (current === owner) { status('Sent. Your reply is saved in this conversation and Archive.'); controls(); }
      $('refresh')?.click();
      return;
    }
    if (result.state === 'unsent') { owner.draft.locked = false; owner.draft.request_id = null; }
    else if (result.can_edit) { owner.draft.locked = false; owner.draft.request_id = null; }
    else { owner.draft.locked = true; }
    owner.draft.error = result.error || (result.state === 'unsent' ? '' : 'send_unconfirmed');
    memory.set(owner.id, owner.draft);
    try { sessionStorage.setItem(prefix + owner.id, JSON.stringify(owner.draft)); } catch {}
    if (current === owner) { status(messages[owner.draft.error] || '', Boolean(owner.draft.error)); controls(); }
  }
  async function open() {
    const id = selected();
    if (!id || opening || busy) return;
    if (document.body.dataset.cmailDrafts === '1' && !getDraft(id)?.locked) {
      document.dispatchEvent(new CustomEvent('cmail:reply-draft', { detail: { id, legacy: getDraft(id) } })); return;
    }
    if (composer && current?.id === id) { composer.querySelector('textarea').focus(); return; }
    opening = true; trigger.disabled = true; trigger.textContent = 'Opening reply…';
    try {
      const context = await api('messages/' + id + '/reply');
      if (selected() !== id) return;
      const draft = getDraft(id) || { text: context.signature ? '\n\n' + context.signature : '', request_id: null, locked: false, error: '' };
      current = { id, context, draft, sent: false };
      composer = document.createElement('form'); composer.className = 'cmail-reply-composer'; composer.setAttribute('aria-label', 'Reply composer');
      composer.innerHTML = '<div class="cmail-reply-heading"><strong>Reply</strong><button class="cmail-reply-cancel" type="button">Cancel</button></div><div class="cmail-reply-address"><span>From</span><span class="cmail-reply-from"></span></div><div class="cmail-reply-address"><span>To</span><span class="cmail-reply-to"></span></div><div class="cmail-reply-subject"></div><textarea rows="8" maxlength="100000" aria-label="Reply message" placeholder="Write your reply…"></textarea><div class="cmail-reply-footer"><button type="submit" class="cmail-reply-send">Send</button><span class="cmail-reply-status" role="status" aria-live="polite"></span></div>';
      composer.querySelector('.cmail-reply-from').textContent = context.from;
      composer.querySelector('.cmail-reply-to').textContent = context.to.join(', ');
      composer.querySelector('.cmail-reply-subject').textContent = context.subject;
      const textarea = composer.querySelector('textarea'); textarea.value = draft.text;
      textarea.addEventListener('input', () => { current.draft.text = textarea.value; current.draft.error = ''; save(); status('Draft saved in this tab.'); controls(); });
      composer.querySelector('.cmail-reply-cancel').addEventListener('click', () => close());
      composer.addEventListener('submit', async (event) => {
        event.preventDefault();
        if (busy || !current || current.sent || textarea.value.trim() === context.signature.trim() || !textarea.value.trim()) return;
        const owner = current;
        owner.draft.text = textarea.value;
        owner.draft.request_id ||= crypto.randomUUID(); owner.draft.locked = true;
        busy = true; save(); controls(); status('Sending…');
        try { await applyReceipt(await api('messages/' + id + '/reply', { request_id: owner.draft.request_id, text: owner.draft.text }), owner); }
        catch (error) { owner.draft.error = 'send_unconfirmed'; if (current === owner) { save(); status(error.message, true); } }
        finally { busy = false; if (current === owner) controls(); update(); }
      });
      $('messageBody').before(composer);
      if (!history.state?.cmailReply) history.pushState({ cmailReply: id }, '', location.href);
      controls(); status(messages[draft.error] || '');
      composer.scrollIntoView({ block: 'center' }); textarea.focus(); textarea.setSelectionRange(0, 0);
      const owner = current;
      if (draft.locked && draft.request_id) {
        try { await applyReceipt(await api('replies/' + draft.request_id), owner); }
        catch (error) { if (current === owner) status(error.message, true); }
      }
    } catch (error) {
      $('readerNotice').textContent = error.message; $('readerNotice').hidden = false;
    } finally { opening = false; trigger.disabled = busy; update(); }
  }
  function update() {
    const id = selected();
    if (activeId !== id) { activeId = id; if (composer && current?.id !== id) close(false); }
    const label = opening ? 'Opening reply…' : busy ? 'Sending…' : getDraft(id)?.text ? 'Continue reply' : 'Reply';
    if (trigger.textContent !== label) trigger.textContent = label;
    trigger.disabled = opening || busy || !id;
  }
  trigger.addEventListener('click', open);
  document.addEventListener('cmail:reply-draft-saved', (event) => { clearDraft(event.detail.id); update(); });
  $('back').addEventListener('click', (event) => { if (composer) { event.preventDefault(); event.stopImmediatePropagation(); close(); } }, true);
  document.addEventListener('keydown', (event) => {
    if (document.querySelector('.cmail-compose-dialog[open]')) return;
    if (event.key === 'Escape' && composer) { event.preventDefault(); event.stopImmediatePropagation(); close(); }
  }, true);
  window.addEventListener('popstate', () => { if (composer && history.state?.cmailReply !== current?.id) close(false); });
  $('signOut').addEventListener('click', () => {
    memory.clear();
    try { for (const key of Object.keys(sessionStorage)) if (key.startsWith(prefix)) sessionStorage.removeItem(key); } catch {}
  }, true);
  new MutationObserver(update).observe($('message'), { childList: true, subtree: true, attributes: true, attributeFilter: ['hidden', 'href'] });
  update();
}

export const replyScript = '(' + replyApplication.toString() + ')();';
export const replyStyles = `
.cmail-reply-trigger,.cmail-reply-send{background:var(--accent);color:var(--bg);border:1px solid var(--accent);border-radius:7px;font:inherit;font-size:13px;font-weight:700;padding:8px 17px;cursor:pointer}
.reader-toolbar .cmail-reply-trigger{padding:6px 15px;margin-right:8px;font-size:13px;white-space:nowrap}
.cmail-reply-composer{margin:22px 0 30px;padding:20px;border:1px solid #526445;border-radius:10px;background:var(--panel2);box-shadow:0 8px 30px #0002;scroll-margin-top:85px}
.cmail-reply-heading{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;font-size:16px}
.cmail-reply-cancel{border:0;background:transparent;color:var(--muted);font:inherit;padding:6px 8px;cursor:pointer}
.cmail-reply-address{display:flex;gap:14px;align-items:baseline;margin:5px 0;font-size:12px;overflow-wrap:anywhere}
.cmail-reply-address>span:first-child{color:var(--muted);min-width:32px}
.cmail-reply-subject{margin:12px 0;color:var(--muted);font-size:12px;overflow-wrap:anywhere}
.cmail-reply-composer textarea{display:block;width:100%;box-sizing:border-box;min-height:190px;resize:vertical;border:1px solid var(--line);border-radius:7px;background:var(--bg);color:var(--text);font:inherit;font-size:14px;line-height:1.6;padding:14px;outline:none}
.cmail-reply-composer textarea:focus{border-color:var(--accent);box-shadow:0 0 0 2px #d5edb51c}
.cmail-reply-footer{display:flex;gap:15px;align-items:center;margin-top:14px}
.cmail-reply-status{font-size:12px;color:var(--muted);line-height:1.5}
.cmail-reply-status.cmail-reply-error{color:#f2c4a0}
.cmail-reply-composer button:disabled,.cmail-reply-trigger:disabled{opacity:.5;cursor:not-allowed}
.cmail-reply-composer [hidden]{display:none}
@media(max-width:700px){.cmail-reply-composer{padding:14px}.cmail-reply-footer{align-items:flex-start}.reader-toolbar{flex-wrap:wrap}}
`;
