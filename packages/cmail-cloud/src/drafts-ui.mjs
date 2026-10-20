function draftsApplication() {
  const $ = (id) => document.getElementById(id);
  const panel = document.createElement('main'); panel.id = 'draftsPanel'; panel.className = 'cmail-drafts-panel'; panel.setAttribute('aria-label', 'Drafts'); panel.hidden = true;
  panel.innerHTML = `<header><div><h1>Drafts</h1><p>Saved emails stay here until you send or discard them.</p></div><button id="newDraftButton" type="button" class="cmail-reply-trigger">New draft</button></header><p id="draftListStatus" role="status"></p><div id="draftRows"></div><nav aria-label="Draft pages"><button id="draftPrevious" type="button">Previous</button><button id="draftNext" type="button">Next</button></nav>`;
  document.querySelector('.workspace').after(panel);
  let cursor = '', previous = [], next = null, controller = null, generation = 0, lastFocus = null;
  const active = () => $('mailView')?.value === 'drafts';
  $('newDraftButton').addEventListener('click', () => document.dispatchEvent(new CustomEvent('cmail:new-draft')));
  function row(item) {
    const button = document.createElement('button'); button.type = 'button'; button.className = 'cmail-draft-row'; button.dataset.draftId = item.id;
    const recipients = item.to.join(', ') || item.pending || 'No recipients yet';
    for (const [tag, className, text] of [['span', 'cmail-draft-to', 'To: ' + recipients], ['strong', 'cmail-draft-subject', item.subject || '(No subject)'],
      ['span', 'cmail-draft-preview', item.preview || 'Empty draft'], ['span', 'cmail-draft-meta', 'From: ' + item.sender + (item.kind === 'reply' ? ' · Reply' : '') + (item.state === 'sending' ? ' · Check send confirmation' : '')],
      ['time', 'cmail-draft-date', new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(item.updated_at))]]) {
      const element = document.createElement(tag); element.className = className; element.textContent = text; button.append(element);
    }
    button.addEventListener('click', () => { lastFocus = item.id; document.dispatchEvent(new CustomEvent('cmail:open-draft', { detail: { id: item.id } })); });
    return button;
  }
  async function load(reset = true) {
    panel.hidden = !active(); controller?.abort(); const token = ++generation;
    if (!active()) return;
    if (reset) { cursor = ''; previous = []; }
    controller = new AbortController(); $('draftListStatus').textContent = 'Loading drafts…';
    $('draftPrevious').disabled = true; $('draftNext').disabled = true; $('refresh').disabled = true;
    try {
      const parameters = new URLSearchParams({ mailbox: $('mailbox').value, query: $('search').value.trim(), cursor });
      const response = await fetch('/cmail/api/drafts?' + parameters, { credentials: 'same-origin', cache: 'no-store', signal: controller.signal });
      if (response.redirected || !response.ok || !response.headers.get('Content-Type')?.includes('application/json')) throw new Error('Drafts could not be loaded. Refresh to try again or sign in.');
      const data = await response.json(); if (token !== generation || !active()) return;
      $('draftRows').replaceChildren(...data.items.map(row)); next = data.next_cursor;
      $('draftListStatus').textContent = data.items.length ? `${data.items.length} shown · ${data.total} saved ${data.total === 1 ? 'draft' : 'drafts'}` : next ? 'No matches on this page. Choose Next to keep looking.' : $('search').value.trim() ? 'No matching drafts.' : 'No saved drafts yet. Choose New draft to start one.';
      $('connectionText').textContent = 'Cloud drafts'; $('connection').title = 'Cloud drafts';
    } catch (error) { if (token === generation && error.name !== 'AbortError') $('draftListStatus').textContent = error.message; }
    finally { if (token === generation && active()) { $('draftPrevious').disabled = !previous.length; $('draftNext').disabled = !next; $('refresh').disabled = false; } }
  }
  $('draftPrevious').addEventListener('click', () => { cursor = previous.pop() || ''; load(false); });
  $('draftNext').addEventListener('click', () => { if (next) { previous.push(cursor); cursor = next; load(false); } });
  document.addEventListener('cmail:drafts-load', () => load());
  document.addEventListener('cmail:drafts-changed', () => { if (active()) load(); });
  document.addEventListener('cmail:composer-closed', () => { if (active()) (panel.querySelector(`[data-draft-id="${lastFocus}"]`) || $('newDraftButton')).focus(); });
  $('signOut').addEventListener('click', () => { controller?.abort(); generation++; $('draftRows')?.replaceChildren(); }, true);
  if (active()) load();
}
export const draftsScript = '(' + draftsApplication.toString() + ')();';
export const draftsStyles = `
body.drafts-view .workspace,body.drafts-view #dateFilters,body.drafts-view .bulk-menu{display:none!important}
.cmail-drafts-panel{padding:24px 28px;overflow:auto;max-height:calc(100dvh - 64px);box-sizing:border-box}
.cmail-drafts-panel[hidden]{display:none}.cmail-drafts-panel>header{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-bottom:22px}
.cmail-drafts-panel h1{font-size:23px;margin:0 0 7px}.cmail-drafts-panel p{font-size:13px;color:var(--muted);margin:0;line-height:1.5}
#draftListStatus{margin-bottom:12px}.cmail-draft-row{display:grid;grid-template-columns:minmax(160px,1fr) minmax(220px,2fr) auto;gap:7px 22px;width:100%;padding:17px 14px;text-align:left;border:0;border-bottom:1px solid var(--line);background:transparent;color:var(--text);font:inherit;cursor:pointer}
.cmail-draft-row:hover,.cmail-draft-row:focus-visible{background:var(--panel2)}.cmail-draft-row:focus-visible{outline:2px solid var(--accent);outline-offset:-2px}
.cmail-draft-to,.cmail-draft-subject{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.cmail-draft-preview{grid-column:2;grid-row:2;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px}
.cmail-draft-meta{grid-column:1;grid-row:2;font-size:11px;color:var(--muted);overflow-wrap:anywhere}.cmail-draft-date{grid-column:3;grid-row:1;font-size:12px;color:var(--muted)}
.cmail-drafts-panel nav{display:flex;justify-content:flex-end;gap:10px;padding:18px 0}.cmail-drafts-panel nav button,.cmail-draft-save{background:var(--panel2);color:var(--text);border:1px solid var(--line);border-radius:6px;padding:8px 13px;font:inherit;cursor:pointer}
.cmail-drafts-panel button:disabled{opacity:.5;cursor:not-allowed}.cmail-compose-form #composeSave{color:var(--accent);border:1px solid #526445;padding:8px 13px}
@media(max-width:700px){.cmail-drafts-panel{padding:18px 12px}.cmail-drafts-panel>header{align-items:flex-start}.cmail-draft-row{grid-template-columns:1fr;gap:7px}.cmail-draft-row>*{grid-column:1;grid-row:auto}.cmail-draft-subject{grid-row:1}.cmail-draft-preview{grid-row:3}.cmail-draft-date{font-size:11px}}
`;
