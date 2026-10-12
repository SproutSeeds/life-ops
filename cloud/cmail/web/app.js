const $ = (id) => document.getElementById(id);
let cursor = null;
let selectedId = null;
let requestGeneration = 0;
let readingGeneration = 0;
let loading = false;
let shown = 0;

function notice(message) { $('notice').textContent = message; $('notice').hidden = !message; }
function connected(value) { $('connectionText').textContent = value; }
async function api(path) {
  const result = await fetch(`/cmail/api/${path}`, { cache: 'no-store', redirect: 'manual' });
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
  if (loading && !reset) return;
  if (reset) { requestGeneration += 1; cursor = null; shown = 0; $('messages').replaceChildren(); notice(''); }
  const generation = requestGeneration;
  loading = true; $('loadMore').disabled = true; $('refresh').disabled = true; connected('Loading mail');
  const query = $('search').value.trim();
  const parameters = new URLSearchParams({ query, mailbox: $('mailbox').value.trim(), after: $('after').value, before: $('before').value });
  if (cursor) parameters.set('cursor', cursor);
  try {
    const result = await api(`messages?${parameters}`);
    if (generation !== requestGeneration) return;
    document.querySelector('.empty-list')?.remove();
    result.items.forEach(appendRow); shown += result.items.length; cursor = result.next_cursor;
    $('total').textContent = new Intl.NumberFormat().format(result.total_stored);
    $('resultCount').textContent = `${shown} shown`; $('listLabel').textContent = query ? 'Search results' : 'Latest messages';
    $('loadMore').hidden = !cursor; $('loadMore').textContent = query ? 'Search older messages' : 'Load older messages';
    if (result.failed) notice('Some messages could not be opened. Other messages are still available.');
    if (!shown) addText($('messages'), 'div', 'empty-list', cursor
      ? 'No matches in this part of your inbox. Choose “Search older messages” to keep looking.'
      : 'No messages match these filters.');
    connected('Cloud inbox');
  } catch (error) {
    if (generation !== requestGeneration) return;
    notice(error.message); connected('Connection interrupted');
  } finally {
    if (generation === requestGeneration) { loading = false; $('loadMore').disabled = false; $('refresh').disabled = false; }
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
$('mailbox').addEventListener('change', () => load(true));
$('applyDates').addEventListener('click', () => load(true));
$('loadMore').addEventListener('click', () => load());
$('back').addEventListener('click', () => {
  readingGeneration += 1; selectedId = null; document.body.classList.remove('reading');
  window.history.replaceState(null, '', window.location.pathname + window.location.search);
  $('message').hidden = true; $('readerPlaceholder').hidden = false;
  document.querySelectorAll('.message-row').forEach((row) => row.classList.remove('selected'));
});
$('allMail').addEventListener('click', () => { $('search').value = ''; $('mailbox').value = ''; $('after').value = ''; $('before').value = ''; load(true); });
api('session').then((session) => { $('accountEmail').textContent = session.email; }).catch((error) => notice(error.message));
function openLinkedMessage() {
  const linked = window.location.hash.match(/^#(mail_\d{16})$/);
  if (linked) return openMessage(linked[1]);
}
window.addEventListener('hashchange', openLinkedMessage);
load(true).then(openLinkedMessage);
