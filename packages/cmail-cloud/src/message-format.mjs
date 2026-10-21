// Presentation only. Archived MIME, message.text, Copy and attachments stay exact.
export function readableBlocks(value) {
  const lines = String(value ?? '').replace(/\r\n?/g, '\n').split('\n');
  const blank = /^[\s\u200b-\u200f\ufeff]*$/u;
  const blocks = [];
  let pending = [], fence = null;
  function flush(pre = false) {
    if (!pending.length) return;
    const quote = !pre && pending.every((line) => /^\s*>/.test(line));
    const list = !pre && !quote && /^\s*(?:[-*+•]|\d+[.)])\s+\S/.test(pending[0]);
    const aligned = pending.length > 1 && pending.filter((line) => /\S(?: {2,}|\t+)\S/.test(line)).length > 1;
    const code = pending.some((line) => /^\s*(?:```|~~~|(?:async\s+)?(?:def|class|function)\s+|(?:const|let|var)\s+\w+\s*=|(?:if|for|while|try|except|catch)\b.*[:{]\s*$|[{}[\]]\s*[,;]?$)/.test(line));
    const tabs = pending.some((line) => /^\t+\S/.test(line));
    const type = pre || !quote && (aligned || code || tabs) ? 'pre' : quote ? 'quote' : list ? 'list' : 'paragraph';
    let content = pending;
    if (type === 'quote') {
      // Keep literal > markers too: a comparison must never change meaning.
      const emptyQuote = (line) => blank.test(line.replace(/^(?:\s*>)+\s*/, ''));
      content = pending.map((line) => line.trimStart());
      content = content.filter((line, index) => !emptyQuote(line) || index > 0 && !emptyQuote(content[index - 1]));
      while (content.length && emptyQuote(content.at(-1))) content.pop();
    }
    else if (type === 'list') {
      const indent = pending.reduce((minimum, line) => Math.min(minimum, line.match(/^ */)[0].length), Infinity);
      content = pending.map((line) => line.slice(indent));
    } else if (type === 'paragraph') content = pending.map((line) => line.trimStart());
    blocks.push({ type, text: content.join('\n') }); pending = [];
  }
  for (const original of lines) {
    const marker = original.match(/^\s*(`{3,}|~{3,})/);
    if (fence) {
      pending.push(original);
      if (marker && marker[1][0] === fence[0] && marker[1].length >= fence.length) { flush(true); fence = null; }
    } else if (marker) {
      flush(); fence = marker[1]; pending.push(original);
    } else if (blank.test(original)) flush();
    else {
      const line = original.replace(/[\t \u00a0]+$/g, '');
      // Keep quoted replies visually distinct even without a blank separator.
      if (pending.length && /^\s*>/.test(line) !== /^\s*>/.test(pending[0])) flush();
      pending.push(line);
    }
  }
  flush(Boolean(fence));
  return blocks;
}

export function readableLinkLabel(value) {
  if (value.length <= 100) return value;
  try {
    const url = new URL(value);
    const path = url.pathname === '/' ? '' : url.pathname;
    const label = url.hostname + (url.port ? ':' + url.port : '') + path;
    return (label.length > 84 ? label.slice(0, 81) + '…' : label) + (url.search || url.hash ? '…' : '');
  } catch { return value; }
}

// Only text nodes and locally-created elements enter the reader. Email content
// never becomes HTML, CSS, an image request or executable page code.
export function createMessageRenderer(blocksFor, labelFor) {
  function appendLinks(target, value, original, budget) {
    let offset = 0;
    for (const match of value.matchAll(/https?:\/\/[^\s<>"\u0000-\u001f]+/g)) {
      if (budget.remaining-- <= 0) break;
      target.append(document.createTextNode(value.slice(offset, match.index)));
      const link = document.createElement('a');
      link.href = match[0]; link.textContent = original ? match[0] : labelFor(match[0]);
      link.title = match[0]; link.target = '_blank'; link.rel = 'noopener noreferrer';
      target.append(link); offset = match.index + match[0].length;
    }
    target.append(document.createTextNode(value.slice(offset)));
  }
  return function renderMessage(value, target, original = false) {
    const text = String(value ?? ''), budget = { remaining: 400 };
    target.replaceChildren();
    target.classList.toggle('cmail-readable', !original);
    target.classList.toggle('cmail-original', original);
    if (original) { appendLinks(target, text, true, budget); return; }
    const blocks = blocksFor(text), fragment = document.createDocumentFragment();
    // Bound DOM work for huge machine-generated mail without dropping content.
    if (blocks.length > 1000) blocks.splice(1000, blocks.length - 1000, {
      type: 'pre', text: blocks.slice(1000).map((block) => block.text).join('\n\n'),
    });
    for (const block of blocks) {
      const element = document.createElement(block.type === 'quote' ? 'blockquote' : block.type === 'pre' ? 'pre' : 'p');
      element.className = 'cmail-text-' + block.type;
      if (block.type === 'pre') element.textContent = block.text;
      else appendLinks(element, block.text, false, budget);
      fragment.append(element);
    }
    target.append(fragment);
  };
}

export const messageRendererSource = '(' + createMessageRenderer.toString() + ')(' + readableBlocks.toString() + ',' + readableLinkLabel.toString() + ')';

export const messageStyles = `
.message-content{max-width:1100px;padding-top:24px;padding-bottom:36px}
.message-content .eyebrow{margin-bottom:12px}.message-content h2{margin-bottom:18px}
.message-content .message-sender{padding-bottom:16px}
.attachments:empty{display:none}
.message-body{line-height:1.6;margin-top:18px}
.message-body.cmail-readable{white-space:normal}
.message-body.cmail-original{white-space:pre-wrap}
.cmail-readable>p,.cmail-readable>blockquote,.cmail-readable>pre{margin:0 0 .9em;white-space:pre-wrap;overflow-wrap:anywhere}
.cmail-readable>:last-child{margin-bottom:0}
.cmail-readable>.cmail-text-quote{margin-left:0;padding-left:14px;border-left:2px solid #526445;color:var(--muted)}
.cmail-readable>.cmail-text-pre{font:12px/1.5 ui-monospace,SFMono-Regular,Consolas,monospace;tab-size:4}
.message-body a{cursor:pointer}#messageFormat{white-space:nowrap}#messageFormat[aria-pressed=true]{color:var(--accent);background:var(--panel2)}
@media(max-width:620px){.message-content{padding-top:20px}.message-body{font-size:14px}.cmail-readable>.cmail-text-quote{padding-left:10px}}
`;
