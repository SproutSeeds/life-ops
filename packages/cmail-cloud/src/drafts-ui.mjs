function draftsApplication() {
  const $ = (id) => document.getElementById(id);
  const panel = document.createElement('main'); panel.id = 'draftsPanel'; panel.className = 'cmail-drafts-panel'; panel.setAttribute('aria-label', 'Drafts'); panel.hidden = true;
  panel.innerHTML = `<section class="cmail-draft-list" aria-label="Saved drafts"><header class="list-toolbar"><h1>Drafts</h1><button id="newDraftButton" type="button" class="cmail-reply-trigger">New draft</button></header><p id="draftListStatus" role="status"></p><div id="draftRows"></div><nav class="pagination" aria-label="Draft pages"><button id="draftPrevious" type="button">Previous</button><button id="draftNext" type="button">Next</button></nav></section><section class="cmail-draft-reader" aria-label="Draft editor"><div id="draftPlaceholder"><h2>Select a draft</h2><p>Open a saved email to continue writing, or choose New draft.</p></div><div id="draftEditor"></div></section>`;
  document.querySelector('.workspace').after(panel);
  let cursor = '', previous = [], next = null, controller = null, generation = 0, lastFocus = null, selected = null;
  const active = () => $('mailView')?.value === 'drafts';
  function selection() {
    for (const button of $('draftRows').children) {
      const chosen = button.dataset.draftId === selected;
      button.classList.toggle('selected', chosen);
      if (chosen) button.setAttribute('aria-current', 'true'); else button.removeAttribute('aria-current');
    }
  }
  $('newDraftButton').addEventListener('click', () => document.dispatchEvent(new CustomEvent('cmail:new-draft')));
  function row(item) {
    const button = document.createElement('button'); button.type = 'button'; button.className = 'cmail-draft-row'; button.dataset.draftId = item.id;
    const recipients = item.to.join(', ') || item.pending || 'No recipients yet';
    for (const [tag, className, text] of [['span', 'cmail-draft-to', 'To: ' + recipients], ['strong', 'cmail-draft-subject', item.subject || '(No subject)'],
      ['span', 'cmail-draft-preview', item.preview || 'Empty draft'], ['span', 'cmail-draft-meta', 'From: ' + item.sender + (item.kind === 'reply' ? ' · Reply' : '') + (item.state === 'sending' ? ' · Check send confirmation' : '')],
      ['time', 'cmail-draft-date', new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric' }).format(new Date(item.updated_at))]]) {
      const element = document.createElement(tag); element.className = className; element.textContent = text; button.append(element);
      if (tag === 'time') { element.dateTime = item.updated_at; element.title = new Date(item.updated_at).toLocaleString(); }
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
      const focused = $('draftRows').contains(document.activeElement) ? document.activeElement.closest('[data-draft-id]')?.dataset.draftId : null;
      $('draftRows').replaceChildren(...data.items.map(row)); selection(); next = data.next_cursor;
      if (focused) (panel.querySelector(`[data-draft-id="${focused}"]`) || $('newDraftButton')).focus();
      $('draftListStatus').textContent = data.items.length ? `${data.items.length} shown · ${data.total} saved ${data.total === 1 ? 'draft' : 'drafts'}` : next ? 'No matches on this page. Choose Next to keep looking.' : $('search').value.trim() ? 'No matching drafts.' : 'No saved drafts yet. Choose New draft to start one.';
      $('connectionText').textContent = 'Cloud drafts'; $('connection').title = 'Cloud drafts';
    } catch (error) { if (token === generation && error.name !== 'AbortError') $('draftListStatus').textContent = error.message; }
    finally { if (token === generation && active()) { $('draftPrevious').disabled = !previous.length; $('draftNext').disabled = !next; $('refresh').disabled = false; } }
  }
  $('draftPrevious').addEventListener('click', () => { cursor = previous.pop() || ''; load(false); });
  $('draftNext').addEventListener('click', () => { if (next) { previous.push(cursor); cursor = next; load(false); } });
  document.addEventListener('cmail:drafts-load', () => load());
  document.addEventListener('cmail:drafts-changed', () => { if (active()) load(); });
  document.addEventListener('cmail:draft-selection', (event) => {
    selected = event.detail.id; if (selected) lastFocus = selected;
    panel.classList.toggle('draft-editing', event.detail.editing);
    $('draftPlaceholder').hidden = event.detail.editing;
    selection();
  });
  document.addEventListener('cmail:composer-closed', () => { if (active()) (panel.querySelector(`[data-draft-id="${lastFocus}"]`) || $('newDraftButton')).focus(); });
  $('signOut').addEventListener('click', () => { controller?.abort(); generation++; $('draftRows')?.replaceChildren(); }, true);
  if (active()) load();
  document.dispatchEvent(new CustomEvent('cmail:drafts-ready'));
}
export const draftsScript = '(' + draftsApplication.toString() + ')();';
export const draftsStyles = `
body.drafts-view .workspace,body.drafts-view #dateFilters,body.drafts-view .bulk-menu{display:none!important}
.cmail-drafts-panel{display:grid;grid-template-columns:390px minmax(0,1fr);flex:1;min-height:0;box-sizing:border-box}
.cmail-drafts-panel[hidden]{display:none}.cmail-draft-list{display:flex;flex-direction:column;min-height:0;min-width:0;background:var(--panel);border-right:1px solid var(--line)}
.cmail-draft-list>header{min-height:48px;justify-content:space-between}.cmail-draft-list #newDraftButton{font-size:11px;padding:7px 11px}
.cmail-drafts-panel p{font-size:12px;color:var(--muted);margin:0;line-height:1.5}
#draftListStatus{padding:0 16px 10px;font-size:10px}#draftRows{flex:1;min-height:0;overflow:auto}
.cmail-draft-row{position:relative;display:grid;grid-template-columns:minmax(0,1fr) auto;gap:4px 10px;width:100%;padding:12px 16px;text-align:left;border:0;border-top:1px solid var(--line);background:#101a14;color:var(--text);font:inherit;cursor:pointer}
.cmail-draft-row:hover,.cmail-draft-row:focus-visible{background:var(--panel2)}.cmail-draft-row:focus-visible{outline:2px solid var(--accent);outline-offset:-2px}
.cmail-draft-row.selected{background:#2b3b26}.cmail-draft-row.selected:before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--accent)}
.cmail-draft-to,.cmail-draft-subject,.cmail-draft-preview,.cmail-draft-meta{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.cmail-draft-to{font-size:12px;grid-column:1;grid-row:1}.cmail-draft-subject{grid-column:1/-1;grid-row:2;font-size:12px;font-weight:550}
.cmail-draft-preview{grid-column:1/-1;grid-row:3;color:var(--muted);font-size:12px}
.cmail-draft-meta{grid-column:1/-1;grid-row:4;font-size:10px;color:var(--muted)}.cmail-draft-date{grid-column:2;grid-row:1;font-size:10px;color:var(--muted)}
.cmail-drafts-panel nav{justify-content:flex-end}
.cmail-draft-reader{min-width:0;min-height:0;display:flex;flex-direction:column;overflow:auto}#draftEditor{flex:1;min-height:0;display:flex;flex-direction:column}
#draftPlaceholder{margin:auto;padding:32px;text-align:center;color:var(--muted)}#draftPlaceholder h2{font-size:18px;color:var(--text);font-weight:550}
.cmail-compose-inline{flex:1;min-height:0;overflow:auto}.cmail-compose-inline .cmail-compose-form{padding:0;min-height:100%;gap:12px}
.cmail-compose-inline .cmail-compose-form>header{position:sticky;top:0;z-index:2;flex-shrink:0;min-height:48px;padding:7px 20px;border-bottom:1px solid var(--line);background:var(--bg);justify-content:flex-start}
.cmail-compose-inline #composeCancel{order:-1;color:var(--muted)}.cmail-compose-inline h2{font-size:12px;font-weight:550;color:var(--muted)}
.cmail-compose-inline .cmail-compose-form>:not(header){width:calc(100% - 64px);max-width:900px;align-self:center;margin:0}
.cmail-compose-inline .cmail-compose-form>.cmail-compose-hint{padding-left:42px}
.cmail-compose-inline .cmail-compose-body{flex:1;display:flex;min-height:260px}.cmail-compose-inline .cmail-compose-body textarea{height:auto;min-height:260px;flex:1;resize:vertical}
.cmail-compose-inline .cmail-compose-form>footer{position:sticky;bottom:0;z-index:1;background:var(--bg);padding:12px 0 20px}
.cmail-drafts-panel button:disabled{opacity:.5;cursor:not-allowed}.cmail-compose-form #composeSave{color:var(--accent);border:1px solid #526445;padding:8px 13px}
@media(max-width:1100px){.cmail-drafts-panel{grid-template-columns:340px minmax(0,1fr)}}
@media(max-width:900px){.cmail-drafts-panel{grid-template-columns:310px minmax(0,1fr)}.cmail-compose-inline .cmail-compose-form>:not(header){width:calc(100% - 32px)}}
@media(max-width:620px){.cmail-drafts-panel{display:flex;flex-direction:column}.cmail-draft-list{flex:1;border:0}.cmail-draft-reader{display:none}.draft-editing .cmail-draft-list{display:none}.draft-editing .cmail-draft-reader{display:flex;flex:1}.cmail-compose-inline .cmail-compose-form>header{padding:7px 10px}.cmail-compose-inline .cmail-compose-field input,.cmail-compose-inline .cmail-compose-field select,.cmail-compose-inline textarea{font-size:16px}.cmail-compose-inline .cmail-compose-form>footer{padding-bottom:calc(16px + env(safe-area-inset-bottom))}}
`;
