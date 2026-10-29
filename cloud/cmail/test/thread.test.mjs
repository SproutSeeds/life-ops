import test from 'node:test';
import assert from 'node:assert/strict';
import { messageIdTokens, conversationNodes, formatMessage, formatThread } from '../web/thread.mjs';

test('conversation links span reply chains and siblings without grouping by subject', () => {
  const nodes = [
    { id: '1', message_id: '<root@example.com>', date: '2026-10-01T10:00:00Z', subject: 'Project' },
    { id: '2', message_id: '<a@example.com>', in_reply_to: '<root@example.com>', date: '2026-10-01T11:00:00Z' },
    { id: '3', message_id: '<b@example.com>', in_reply_to: '<a@example.com>', date: '2026-10-01T12:00:00Z' },
    { id: '4', message_id: '<sibling@example.com>', references: '<root@example.com> <missing@example.com>', date: '2026-10-01T13:00:00Z' },
    { id: '5', message_id: '<unrelated@example.com>', subject: 'Project', date: '2026-10-01T14:00:00Z' },
    { id: '6', subject: 'Project' },
  ];
  const result = conversationNodes(nodes.toReversed(), '2');
  assert.deepEqual(result.nodes.map(node => node.id), ['1','2','3','4']);
  assert.equal(result.missing, 1);
  assert.deepEqual(conversationNodes(nodes, '6').nodes.map(node => node.id), ['6']);
  assert.deepEqual(conversationNodes(nodes, 'absent').nodes, []);
});

test('thread grouping handles missing parents, folded IDs, and duplicate deliveries', () => {
  assert.deepEqual(messageIdTokens('<one@example.com>\r\n <two@example.com> <one@example.com>'), ['<one@example.com>','<two@example.com>']);
  assert.deepEqual(messageIdTokens('one@example.com'), ['<one@example.com>']);
  const nodes = [
    { id: '1', message_id: '<child@example.com>', in_reply_to: '<absent@example.com>', received_at:'2026-10-01T12:00:00Z' },
    { id: '2', message_id: '<child@example.com>', in_reply_to: '<absent@example.com>', received_at:'2026-10-01T12:00:01Z' },
    { id: '3', message_id: '<sibling@example.com>', references:'<absent@example.com>', date:'invalid', received_at:'2026-10-01T13:00:00Z' },
  ];
  const result = conversationNodes(nodes, '2');
  assert.deepEqual(result.nodes.map(node => node.id), ['2','3']); assert.equal(result.missing, 1);
});

test('copy formatting preserves complete bodies, unicode, quoted history, and address context', () => {
  const first = { subject:'Project ✓', from:{name:'Alex',address:'alex@example.com'}, to:[{address:'owner@example.com'}], cc:[{name:'Team',group:[{address:'team@example.com'}]}], date:'2026-10-01T10:00:00Z', text:'Hello café 👋\n\nOn Tuesday, someone wrote:\n> Keep this quoted history.\n\nInline response stays too.' };
  const second = { ...first, subject:'Re: Project ✓', text:'Confirmed.\nLiteral <script>text</script> stays plain text.' };
  const single = formatMessage(first);
  assert(single.includes('From: Alex <alex@example.com>')); assert(single.includes('Cc: team@example.com'));
  assert(single.endsWith(first.text)); assert(!single.includes(second.text));
  const thread = formatThread([first,second]);
  assert(thread.includes('----- Message 1 of 2 -----')); assert(thread.includes('----- Message 2 of 2 -----'));
  assert(thread.indexOf(first.text) < thread.indexOf(second.text)); assert(thread.endsWith(second.text));
});
