import assert from 'node:assert/strict';
import test from 'node:test';
import { readableBlocks, readableLinkLabel, createMessageRenderer, messageRendererSource } from '../src/message-format.mjs';
import { appScript } from '../src/web-app.mjs';

test('email layout padding collapses without losing order, address, amount or paragraph content', () => {
  const message = 'Order confirmation\r\n' + ' \t\u00a0\u200b\r\n'.repeat(38) +
    '                    Thanks for your order!\r\n\r\n\r\n' +
    '                      SHIPPING ADDRESS\r\n\r\n       123 Example Lane\r\nExample Town, ZZ 12345\r\n\r\nTotal: $123.45\r\n';
  const blocks = readableBlocks(message);
  assert.deepEqual(blocks.map((b) => b.text), ['Order confirmation', 'Thanks for your order!', 'SHIPPING ADDRESS', '123 Example Lane\nExample Town, ZZ 12345', 'Total: $123.45']);
  assert(blocks.every((b) => b.type === 'paragraph'));
});

test('list nesting, hard line breaks, Unicode and literal comparisons survive', () => {
  const text = 'Hello Zoë,\nThanks for writing.\n\n  1. First item\n    - Nested item\n  2. Second item\n\n>= 4 and < 9\n\nق\u200dر';
  const blocks = readableBlocks(text);
  assert.equal(blocks[0].text, 'Hello Zoë,\nThanks for writing.');
  assert.deepEqual(blocks[1], { type: 'list', text: '1. First item\n  - Nested item\n2. Second item' });
  assert.equal(blocks[2].text, '>= 4 and < 9'); assert.equal(blocks[3].text, 'ق\u200dر');
});

test('quoted conversations stay visible with bounded empty quote lines and literal markers', () => {
  const blocks = readableBlocks('My answer.\n\n\n\n\n> Earlier message\n> \n> \n> \n> Still relevant.\n> ');
  assert.equal(blocks.length, 2); assert.equal(blocks[1].type, 'quote');
  assert.equal(blocks[1].text, '> Earlier message\n>\n> Still relevant.');
  assert.equal(readableBlocks('> 4')[0].text, '> 4');
});

test('code fences retain exact internal blank lines and indentation', () => {
  const code = '```python\nif ready:\n    print("ready")\n\n\n    print("done")\n```';
  const blocks = readableBlocks('Example:\n\n' + code + '\n\nNext paragraph.');
  assert.deepEqual(blocks[1], { type: 'pre', text: code });
  assert.equal(readableBlocks('```\n\n  incomplete fence')[0].text, '```\n\n  incomplete fence');
});

test('unfenced code, tabs and aligned tables retain their indentation', () => {
  for (const text of ['def answer():\n    return 42', '{\n  "answer": 42\n}', '\tfirst\n\tsecond', 'Item        Qty     Total\nExample     2       12.00']) {
    assert.deepEqual(readableBlocks(text), [{ type: 'pre', text }]);
  }
});

test('long link labels expose the real host and retain ordinary short URLs', () => {
  const long = 'https://example.test/orders/view?id=42&tracking=' + 'a'.repeat(300);
  assert.equal(readableLinkLabel(long), 'example.test/orders/view…');
  assert.equal(readableLinkLabel('https://example.test/page'), 'https://example.test/page');
  assert(readableLinkLabel('https://wrong.test@example.test/' + 'a'.repeat(180)).startsWith('example.test/'));
});

class Node {
  constructor(tag = '#text', value = '') { this.tag = tag; this.value = value; this.children = []; this.classList = { toggle() {} }; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; this.value = ''; }
  set textContent(value) { this.value = value; this.children = []; }
  get textContent() { return this.value + this.children.map((child) => child.textContent).join(''); }
  set innerHTML(_) { throw new Error('Email text must not become HTML'); }
}
function rendered(text, original = false) {
  const saved = globalThis.document;
  globalThis.document = { createElement: (tag) => new Node(tag), createTextNode: (value) => new Node('#text', value), createDocumentFragment: () => new Node('#fragment') };
  try { const target = new Node('div'); createMessageRenderer(readableBlocks, readableLinkLabel)(text, target, original); return target; }
  finally { if (saved === undefined) delete globalThis.document; else globalThis.document = saved; }
}
const descendants = (node) => node.children.flatMap((child) => [child, ...descendants(child)]);

test('untrusted markup stays literal text, without executable elements or remote media', () => {
  const text = '<script>alert(1)</script>\n<img src="https://tracker.example.test/pixel" onerror="alert(2)">\njavascript:alert(3)\n&lt;svg onload=alert(4)&gt;';
  const target = rendered(text), nodes = descendants(target);
  assert(!nodes.some((node) => ['script','img','svg','iframe','style'].includes(node.tag)));
  assert.equal(target.textContent, text);
  assert(nodes.filter((node) => node.tag === 'a').every((node) => /^https?:\/\//.test(node.href)));
});

test('formatted links keep full destinations and Original text is byte-for-byte display content', () => {
  const url = 'https://example.test/page?token=' + 'x'.repeat(200);
  const original = ' \r\n\r\nHi,\r\n\r\n\u00a0\r\n' + url + '\r\n   ';
  const pretty = rendered(original), link = descendants(pretty).find((node) => node.tag === 'a');
  assert.equal(link.href, url); assert.equal(link.title, url); assert.equal(link.rel, 'noopener noreferrer');
  assert.equal(link.textContent, 'example.test/page…');
  assert.equal(rendered(original, true).textContent, original);
});

test('extremely fragmented mail and many links bound DOM work without dropping the tail', () => {
  const text = Array.from({ length: 2500 }, (_, i) => `Paragraph ${i}: https://example.test/${i}`).join('\n\n');
  const target = rendered(text), nodes = descendants(target);
  assert(nodes.filter((node) => node.tag === 'a').length <= 400);
  assert(nodes.filter((node) => ['p','pre','blockquote'].includes(node.tag)).length <= 1001);
  assert(target.textContent.endsWith('Paragraph 2499: https://example.test/2499'));
});

test('empty input and serialized browser code are valid', () => {
  assert.deepEqual(readableBlocks(null), []); assert.deepEqual(readableBlocks(' \n\u00a0\n\u200b'), []);
  assert.doesNotThrow(() => new Function('return ' + messageRendererSource));
  assert.doesNotThrow(() => new Function(appScript));
});
