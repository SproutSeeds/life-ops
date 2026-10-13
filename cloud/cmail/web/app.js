const $ = (id) => document.getElementById(id);
let cursor = null;
let selectedId = null;
let requestGeneration = 0;
let readingGeneration = 0;
let loading = false;
let items = [];
let pageIndex = 0;
let pageSize = 50;
try { const saved = Number(localStorage.getItem('cmail.page-size')); if ([20,50,100].includes(saved)) pageSize = saved; } catch {}
let exhausted = false;
let interrupted = false;
let activeFilters = null;
let totalStored = 0;
let selectedMessage = null;
let renderedIds = '';
const pendingReadIds = new Set();
const readStateOverrides = new Map();
let scanned = 0;
let failed = 0;
let loadController = null;
let signingOut = false;
const knownMailboxes = new Set();

function addMailboxes(addresses) {
  for (const value of addresses) {
    const address = String(value || '').trim().toLowerCase();
    if (!address || knownMailboxes.has(address)) continue;
    knownMailboxes.add(address);
    const option = document.createElement('option'); option.value = address; option.textContent = address;
    $('mailbox').append(option);
  }
  updateInboxLabel();
}
function updateInboxLabel() {
  const mailbox = $('mailbox').value;
  $('inboxTitle').textContent = mailbox ? 'Inbox' : 'All inboxes';
  $('allMail').classList.toggle('active', !mailbox);
  $('search').placeholder = mailbox ? 'Search this inbox' : 'Search all inboxes';
  $('mailboxStatus').textContent = mailbox ? 'Showing mail sent to this address.'
    : `${knownMailboxes.size} ${knownMailboxes.size === 1 ? 'address' : 'addresses'} · One combined inbox`;
}
function closeMessage() {
  readingGeneration += 1; selectedId = null; selectedMessage = null; document.body.classList.remove('reading');
  window.history.replaceState(null, '', window.location.pathname + window.location.search);
  $('message').hidden = true; $('readerPlaceholder').hidden = false;
  document.querySelectorAll('.message-row').forEach((row) => row.classList.remove('selected'));
}
function switchInbox() {
  const url = new URL(window.location.href);
  if ($('mailbox').value) url.searchParams.set('mailbox', $('mailbox').value);
  else url.searchParams.delete('mailbox');
  url.hash = ''; window.history.replaceState(null, '', url);
  closeMessage(); updateInboxLabel(); load(true);
}

function notice(message) { $('notice').textContent = message; $('notice').hidden = !message; }
function connected(value) { $('connectionText').textContent = value; }
async function api(path, signal, options = {}) {
  const result = await fetch(`/cmail/api/${path}`, { ...options, cache: 'no-store', redirect: 'manual', signal });
  if (result.type === 'opaqueredirect' || [401,403].includes(result.status)) {
    throw new Error('Your session has expired. Reload this page to sign in again.');
  }
  if (!result.ok || !result.headers.get('content-type')?.includes('application/json')) {
    throw new Error('Your mail is temporarily unavailable. Please try Refresh.');
  }
  return result.json();
}
function dateLabel(value, full = false) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value || '';
  return new Intl.DateTimeFormat(undefined, full
    ? { dateStyle: 'medium', timeStyle: 'short' }
    : { month: 'short', day: 'numeric' }).format(date);
}
function senderLabel(value) { return String(value).replace(/<[^>]*>/g, '').trim().replace(/^"|"$/g, '').trim() || value; }
function mailboxAddress(value) {
  if (!value) return '';
  if (value.group) return value.group.map(mailboxAddress).join(', ');
  return value.name ? `${value.name} <${value.address}>` : value.address || '';
}
function addText(parent, tag, className, value) {
  const element = document.createElement(tag); element.className = className; element.textContent = value;
  parent.append(element); return element;
}
function applyRowState(row, item) {
  row.classList.toggle('unread', !item.read);
  row.classList.toggle('selected', item.id === selectedId);
  row.dataset.read = String(Boolean(item.read));
  row.setAttribute('aria-label', `${item.read ? 'Read' : 'Unread'}: ${senderLabel(item.from)} — ${item.subject}`);
}
function appendRow(item) {
  const row = document.createElement('button'); row.type = 'button'; row.className = 'message-row'; row.dataset.id = item.id;
  const indicator = addText(row, 'span', 'read-indicator', ''); indicator.setAttribute('aria-hidden', 'true');
  addText(row, 'span', 'row-sender', senderLabel(item.from));
  const subject = addText(row, 'span', 'row-subject', item.subject); subject.title = item.subject;
  addText(row, 'span', 'row-mailbox', item.mailbox);
  addText(row, 'time', 'row-date', dateLabel(item.received_at));
  applyRowState(row, item);
  row.addEventListener('click', () => openMessage(item.id)); $('messages').append(row);
}
function isSearching() { return Boolean(activeFilters?.query || activeFilters?.after || activeFilters?.before); }
function renderList() {
  const start = pageIndex * pageSize;
  const visible = items.slice(start, start + pageSize);
  const ids = visible.map(item => item.id).join(',');
  if (ids !== renderedIds || !visible.length) {
    $('messages').replaceChildren(); renderedIds = ids;
    visible.forEach(appendRow);
    if (!visible.length) {
      const text = loading ? (isSearching() ? 'Searching your mail… Results will appear as they are found.' : 'Loading messages for this inbox…')
        : interrupted ? 'Loading has not finished. Choose Continue to keep looking.'
        : failed ? 'Some messages could not be loaded. Results may be incomplete.'
        : isSearching() ? 'No messages match these filters.'
        : $('mailbox').value ? 'No messages have arrived for this address yet.' : 'No messages have arrived yet.';
      addText($('messages'), 'div', 'empty-list', text);
    }
  } else {
    document.querySelectorAll('.message-row').forEach((row, index) => applyRowState(row, visible[index]));
  }
  const knownTotal = exhausted ? items.length : !isSearching() && !activeFilters?.mailbox && !failed && totalStored ? totalStored : null;
  $('resultCount').textContent = visible.length ? `${(start + 1).toLocaleString()}–${(start + visible.length).toLocaleString()}${knownTotal === null ? ' shown' : ` of ${knownTotal.toLocaleString()}`}` : '0 shown';
  const pageCount = knownTotal === null ? null : Math.max(1, Math.ceil(knownTotal / pageSize));
  $('pageLabel').textContent = `Page ${pageIndex + 1}${pageCount === null ? '' : ` of ${pageCount}`}`;
  $('previousPage').disabled = pageIndex === 0;
  const cachedNext = items.length > start + pageSize;
  $('nextPage').disabled = !cachedNext && (exhausted || loading || interrupted);
  $('continueLoad').hidden = !interrupted || loading;
  $('stopSearch').hidden = !loading;
}
async function load(reset = false) {
  if (signingOut || (loading && !reset)) return;
  if (reset) {
    requestGeneration += 1; loadController?.abort(); cursor = null; items = []; pageIndex = 0;
    scanned = 0; failed = 0; exhausted = false; interrupted = false;
    readStateOverrides.clear();
    activeFilters = { query: $('search').value.trim(), mailbox: $('mailbox').value, after: $('after').value, before: $('before').value };
    renderedIds = ''; $('messages').replaceChildren(); $('messages').scrollTop = 0; notice('');
  }
  if (exhausted) { renderList(); return; }
  const generation = requestGeneration;
  loadController = new AbortController(); const { signal } = loadController;
  const searching = isSearching();
  const parameters = new URLSearchParams(activeFilters);
  loading = true; interrupted = false; $('refresh').disabled = true;
  connected(searching ? 'Searching mail' : 'Loading mail');
  $('listLabel').textContent = searching ? 'Searching your mail…' : 'Loading messages…';
  renderList();
  try {
    do {
      if (cursor) parameters.set('cursor', cursor); else parameters.delete('cursor');
      // One extra match tells us whether another visible page exists.
      const target = (pageIndex + 1) * pageSize + 1;
      parameters.set('limit', String(searching ? pageSize : Math.min(100, Math.max(1, target - items.length))));
      const result = await api(`messages?${parameters}`, signal);
      if (generation !== requestGeneration || signingOut) return;
      addMailboxes(result.items.map(item => item.mailbox));
      for (const item of result.items) if (readStateOverrides.has(item.id)) item.read = readStateOverrides.get(item.id);
      items.push(...result.items); cursor = result.next_cursor; exhausted = !cursor;
      scanned += result.scanned; failed += result.failed; totalStored = Number(result.total_stored);
      $('total').textContent = totalStored.toLocaleString();
      $('listLabel').textContent = searching ? `Searching ${scanned.toLocaleString()} / ${totalStored.toLocaleString()}` : 'Latest messages';
      if (failed) notice('Some messages could not be loaded. Results may be incomplete.');
      renderList();
    } while (cursor && (searching || items.length < (pageIndex + 1) * pageSize + 1));
    if (exhausted) pageIndex = Math.min(pageIndex, Math.max(0, Math.ceil(items.length / pageSize) - 1));
    $('listLabel').textContent = searching ? (failed ? 'Search finished with errors' : 'Search complete') : 'Latest messages';
    connected('Cloud inbox');
  } catch (error) {
    if (generation !== requestGeneration || signingOut) return;
    interrupted = true;
    if (error.name === 'AbortError') {
      $('listLabel').textContent = searching ? 'Search paused' : 'Loading paused'; connected('Paused');
    } else {
      notice(error.message); connected('Connection interrupted'); $('listLabel').textContent = 'Loading interrupted';
    }
  } finally {
    if (generation === requestGeneration && !signingOut) {
      loading = false; $('refresh').disabled = false; renderList();
    }
  }
}
function changePage(delta) {
  pageIndex = Math.max(0, pageIndex + delta); closeMessage(); $('messages').scrollTop = 0; renderList();
  if (!exhausted && items.length < (pageIndex + 1) * pageSize + 1) load();
}
function updateReadControl() {
  if (!selectedMessage) return;
  $('toggleRead').disabled = pendingReadIds.has(selectedMessage.id);
  $('toggleRead').textContent = pendingReadIds.has(selectedMessage.id) ? 'Saving…' : selectedMessage.read ? 'Mark unread' : 'Mark read';
}
async function setReadState(id, read) {
  if (pendingReadIds.has(id) || signingOut) return;
  pendingReadIds.add(id); updateReadControl();
  if (selectedId === id) { $('readerNotice').hidden = true; $('readerNotice').textContent = ''; }
  try {
    const result = await api(`messages/${id}/read-status`, undefined, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ read }),
    });
    if (signingOut) return;
    readStateOverrides.set(id, result.read);
    const item = items.find(item => item.id === id); if (item) item.read = result.read;
    if (selectedMessage?.id === id) selectedMessage.read = result.read;
    renderList();
  } catch {
    if (!signingOut && selectedId === id) {
      $('readerNotice').textContent = 'Read status could not be saved. Use the read/unread button to try again.';
      $('readerNotice').hidden = false;
    }
  } finally {
    pendingReadIds.delete(id); if (!signingOut) updateReadControl();
  }
}
function renderBody(text) {
  const target = $('messageBody'); target.replaceChildren();
  const matches = String(text).matchAll(/https?:\/\/[^\s<>"\u0000-\u001f]+/g);
  let offset = 0;
  for (const match of matches) {
    target.append(document.createTextNode(text.slice(offset, match.index)));
    const link = document.createElement('a'); link.href = match[0]; link.textContent = match[0];
    link.target = '_blank'; link.rel = 'noopener noreferrer'; target.append(link);
    offset = match.index + match[0].length;
  }
  target.append(document.createTextNode(text.slice(offset)));
}
async function openMessage(id) {
  if (signingOut) return;
  const generation = ++readingGeneration;
  notice(''); connected('Opening message');
  try {
    const message = await api(`messages/${encodeURIComponent(id)}`);
    if (generation !== readingGeneration) return;
    selectedId = id; selectedMessage = message;
    $('readerNotice').hidden = true; $('readerNotice').textContent = '';
    window.history.replaceState(null, '', `#${id}`);
    document.querySelectorAll('.message-row').forEach((row) => row.classList.toggle('selected', row.dataset.id === id));
    $('messageSubject').textContent = message.subject; $('messageMailbox').textContent = message.mailbox;
    $('messageFrom').textContent = mailboxAddress(message.from); $('senderAvatar').textContent = (message.from?.name || message.from?.address || 'M').slice(0, 1).toUpperCase();
    $('messageTo').textContent = `To: ${message.to.map(mailboxAddress).join(', ')}${message.cc.length ? ` · Cc: ${message.cc.map(mailboxAddress).join(', ')}` : ''}`;
    $('messageDate').textContent = dateLabel(message.date, true); $('downloadRaw').href = `/cmail/api/messages/${id}/raw`;
    $('attachments').replaceChildren();
    message.attachments.forEach((file) => {
      const link = document.createElement('a'); link.className = 'attachment'; link.href = `/cmail/api/messages/${id}/attachments/${file.index}`;
      link.textContent = `↓ ${file.filename}`; addText(link, 'span', '', `${Math.max(1, Math.round(file.size / 1024))} KB`); $('attachments').append(link);
    });
    renderBody(message.text); $('readerPlaceholder').hidden = true; $('message').hidden = false;
    document.body.classList.add('reading'); document.querySelector('.reader').scrollTop = 0; connected('Cloud inbox');
    const item = items.find(item => item.id === id); if (item) item.read = message.read;
    renderList(); updateReadControl();
    if (!message.read) await setReadState(id, true);
  } catch (error) { if (generation === readingGeneration) { notice(error.message); connected('Connection interrupted'); } }
}
$('searchForm').addEventListener('submit', (event) => { event.preventDefault(); load(true); });
$('refresh').addEventListener('click', () => load(true));
$('mailbox').addEventListener('change', switchInbox);
$('applyDates').addEventListener('click', () => load(true));
$('previousPage').addEventListener('click', () => changePage(-1));
$('nextPage').addEventListener('click', () => changePage(1));
$('continueLoad').addEventListener('click', () => load());
$('pageSize').value = String(pageSize);
$('pageSize').addEventListener('change', () => {
  pageSize = Number($('pageSize').value); pageIndex = 0;
  try { localStorage.setItem('cmail.page-size', String(pageSize)); } catch {}
  closeMessage(); $('messages').scrollTop = 0; renderList();
  if (!exhausted && items.length < pageSize + 1) load();
});
$('toggleRead').addEventListener('click', () => { if (selectedMessage) setReadState(selectedMessage.id, !selectedMessage.read); });
$('stopSearch').addEventListener('click', () => loadController?.abort());
$('signOut').addEventListener('click', (event) => {
  if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  signingOut = true; requestGeneration += 1; readingGeneration += 1; loadController?.abort();
  // Clear rendered mail before leaving, including any page kept in browser history.
  document.body.replaceChildren(); document.title = 'Signing out · C-mail';
});
window.addEventListener('pageshow', (event) => { if (event.persisted) window.location.reload(); });
$('back').addEventListener('click', closeMessage);
$('allMail').addEventListener('click', () => { $('search').value = ''; $('mailbox').value = ''; $('after').value = ''; $('before').value = ''; switchInbox(); });
api('session').then((session) => {
  if (signingOut) return;
  $('accountEmail').textContent = session.email;
  $('signOut').title = `Sign out ${session.email}`;
  addMailboxes(session.mailboxes || []);
}).catch((error) => { if (!signingOut) notice(error.message); });
function openLinkedMessage() {
  if (signingOut) return;
  const linked = window.location.hash.match(/^#(mail_\d{16})$/);
  if (linked) return openMessage(linked[1]);
}
window.addEventListener('hashchange', openLinkedMessage);
const initialMailbox = new URLSearchParams(window.location.search).get('mailbox') || '';
if (initialMailbox) { addMailboxes([initialMailbox]); $('mailbox').value = initialMailbox.trim().toLowerCase(); }
updateInboxLabel();
load(true).then(openLinkedMessage);
