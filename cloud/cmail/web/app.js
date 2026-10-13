const $ = (id) => document.getElementById(id);
let cursor = null;
let selectedId = null;
let requestGeneration = 0;
let readingGeneration = 0;
let loading = false;
let shown = 0;
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
  readingGeneration += 1; selectedId = null; document.body.classList.remove('reading');
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
async function api(path, signal) {
  const result = await fetch(`/cmail/api/${path}`, { cache: 'no-store', redirect: 'manual', signal });
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
function appendRow(item) {
  const row = document.createElement('button'); row.type = 'button'; row.className = 'message-row'; row.dataset.id = item.id;
  if (item.id === selectedId) row.classList.add('selected');
  const top = document.createElement('div'); top.className = 'row-top';
  addText(top, 'span', 'row-sender', senderLabel(item.from)); addText(top, 'time', 'row-date', dateLabel(item.received_at)); row.append(top);
  addText(row, 'div', 'row-subject', item.subject); addText(row, 'div', 'row-mailbox', item.mailbox);
  row.addEventListener('click', () => openMessage(item.id)); $('messages').append(row);
}
async function load(reset = false) {
  if (signingOut) return;
  if (loading && !reset) return;
  if (reset) {
    requestGeneration += 1; loadController?.abort(); cursor = null; shown = 0; scanned = 0; failed = 0;
    $('messages').replaceChildren(); notice('');
  }
  const generation = requestGeneration;
  loadController = new AbortController();
  const { signal } = loadController;
  loading = true; $('loadMore').disabled = true; $('refresh').disabled = true;
  const query = $('search').value.trim();
  const mailbox = $('mailbox').value;
  const parameters = new URLSearchParams({ query, mailbox, after: $('after').value, before: $('before').value });
  const searching = Boolean(query || $('after').value || $('before').value);
  const startingShown = shown;
  let interrupted = false;
  connected(searching ? 'Searching mail' : 'Loading mail');
  $('listLabel').textContent = searching ? (mailbox ? 'Searching this inbox…' : 'Searching all inboxes…') : 'Loading messages…';
  $('resultCount').textContent = searching ? `${shown} found` : `${shown} shown`;
  $('stopSearch').hidden = false;
  $('loadMore').hidden = true;
  try {
    do {
      if (cursor) parameters.set('cursor', cursor);
      else parameters.delete('cursor');
      const result = await api(`messages?${parameters}`, signal);
      if (generation !== requestGeneration || signingOut) return;
      document.querySelector('.empty-list')?.remove();
      addMailboxes(result.items.map((item) => item.mailbox));
      result.items.forEach(appendRow); shown += result.items.length; cursor = result.next_cursor;
      scanned += result.scanned; failed += result.failed;
      $('total').textContent = new Intl.NumberFormat().format(result.total_stored);
      $('resultCount').textContent = searching ? `${shown} found` : `${shown} shown`;
      if (searching) $('listLabel').textContent = `Searching ${scanned.toLocaleString()} / ${Number(result.total_stored).toLocaleString()}`;
      else $('listLabel').textContent = 'Latest messages';
      if (failed) notice('Some messages could not be searched. Results may be incomplete.');
      if (!shown && searching && cursor) addText($('messages'), 'div', 'empty-list', 'Searching older messages… Results will appear here as they are found.');
      else if (!searching && shown === startingShown && cursor) {
        $('listLabel').textContent = 'Looking for messages…';
        addText($('messages'), 'div', 'empty-list', 'Looking through older mail for this inbox…');
      }
    } while (cursor && (searching || shown === startingShown));
    if (searching) $('listLabel').textContent = failed ? 'Search finished with errors' : 'Search complete';
    if (!shown) {
      document.querySelector('.empty-list')?.remove();
      addText($('messages'), 'div', 'empty-list', failed ? 'No matches found in the readable messages. Some messages could not be searched.'
        : searching ? 'No messages match these filters.' : mailbox ? 'No messages have arrived for this address yet.' : 'No messages have arrived yet.');
    }
    connected('Cloud inbox');
  } catch (error) {
    if (generation !== requestGeneration || signingOut) return;
    interrupted = true;
    if (error.name === 'AbortError') {
      $('listLabel').textContent = searching ? 'Search paused' : 'Loading paused'; connected(searching ? 'Search paused' : 'Loading paused');
    } else {
      notice(error.message); connected('Connection interrupted');
      if (searching) $('listLabel').textContent = 'Search interrupted';
    }
    if (!shown) {
      document.querySelector('.empty-list')?.remove();
      addText($('messages'), 'div', 'empty-list', searching
        ? 'The search has not finished. Choose “Continue search” to keep looking.'
        : error.name === 'AbortError' ? 'Loading paused. Choose “Continue loading” to keep looking.' : 'Mail could not be loaded. Please try Refresh.');
    }
  } finally {
    if (generation === requestGeneration && !signingOut) {
      loading = false; $('loadMore').disabled = false; $('refresh').disabled = false; $('stopSearch').hidden = true;
      $('loadMore').hidden = !cursor && !interrupted;
      $('loadMore').textContent = searching ? 'Continue search' : interrupted ? 'Continue loading' : 'Load older messages';
    }
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
    selectedId = id;
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
  } catch (error) { if (generation === readingGeneration) { notice(error.message); connected('Connection interrupted'); } }
}
$('searchForm').addEventListener('submit', (event) => { event.preventDefault(); load(true); });
$('refresh').addEventListener('click', () => load(true));
$('mailbox').addEventListener('change', switchInbox);
$('applyDates').addEventListener('click', () => load(true));
$('loadMore').addEventListener('click', () => load());
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
