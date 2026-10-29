export function messageIdTokens(value) {
  const text = Array.isArray(value) ? value.join(' ') : String(value || '');
  const bracketed = text.match(/<[^<>\s]+>/g);
  return [...new Set(bracketed || text.split(/[\s,]+/).filter(token => token.includes('@')).map(token => `<${token.replace(/^<|>$/g, '')}>`))];
}

export function conversationNodes(nodes, selectedId) {
  const byToken = new Map();
  const tokensFor = node => [...new Set([
    ...messageIdTokens(node.message_id), ...messageIdTokens(node.in_reply_to), ...messageIdTokens(node.references),
  ])];
  for (const node of nodes) for (const token of tokensFor(node)) {
    if (!byToken.has(token)) byToken.set(token, []);
    byToken.get(token).push(node);
  }
  const seed = nodes.find(node => node.id === selectedId);
  if (!seed) return { nodes: [], missing: 0 };
  const found = new Map([[seed.id, seed]]), visited = new Set(), queue = [seed];
  for (let index = 0; index < queue.length; index++) for (const token of tokensFor(queue[index])) {
    if (visited.has(token)) continue;
    visited.add(token);
    for (const node of byToken.get(token) || []) if (!found.has(node.id)) { found.set(node.id, node); queue.push(node); }
  }
  // A message delivered to several connected addresses is copied only once.
  // Keep the selected delivery when there is a duplicate Message-ID.
  const unique = new Map();
  for (const node of found.values()) {
    const key = messageIdTokens(node.message_id)[0] || node.id;
    if (!unique.has(key) || node.id === selectedId) unique.set(key, node);
  }
  const timestamp = node => Date.parse(node.date) || Date.parse(node.received_at) || 0;
  const ordered = [...unique.values()].sort((a, b) => timestamp(a) - timestamp(b) || a.id.localeCompare(b.id));
  const available = new Set([...found.values()].flatMap(node => messageIdTokens(node.message_id)));
  return { nodes: ordered, missing: [...visited].filter(token => !available.has(token)).length };
}

function address(value) {
  if (value?.group) return value.group.map(address).filter(Boolean).join(', ');
  return value?.name ? `${value.name} <${value.address || ''}>` : value?.address || '';
}

export function formatMessage(message) {
  return [
    `Subject: ${message.subject || '(No subject)'}`,
    `From: ${address(message.from)}`,
    `To: ${(message.to || []).map(address).join(', ')}`,
    ...(message.cc?.length ? [`Cc: ${message.cc.map(address).join(', ')}`] : []),
    `Date: ${message.date || message.received_at || ''}`,
    '', message.text || '',
  ].join('\n');
}

export function formatThread(messages) {
  return messages.map((message, index) => `----- Message ${index + 1} of ${messages.length} -----\n${formatMessage(message)}`).join('\n\n');
}
