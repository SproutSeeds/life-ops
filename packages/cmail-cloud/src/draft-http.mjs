import { authorize } from './production-base.mjs';
import { draftsScript, draftsStyles } from './drafts-ui.mjs';

export function withDrafts(worker, checkAccess = authorize) {
  const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff' } });
  return {
    ...worker,
    async fetch(request, env, context) {
      const url = new URL(request.url), path = url.pathname;
      const draft = path.match(/^\/cmail\/api\/drafts\/(draft_[a-f0-9-]{36})(?:\/(send|discard))?$/);
      const reply = path.match(/^\/cmail\/api\/drafts\/reply\/(mail_\d{16})$/);
      const asset = ['/cmail/drafts.js', '/cmail/drafts.css'].includes(path);
      const attachment = path.match(/^\/cmail\/api\/drafts\/attachments\/([a-f0-9-]{36})$/);
      if (asset || path === '/cmail/api/drafts' || path === '/cmail/api/drafts/save' || draft || reply || attachment) {
        const access = await checkAccess(request, env);
        if (!access.ok) return json({ error: access.error }, access.status);
        if (env.CMAIL_DRAFTS_ENABLED !== '1') return json({ error: 'not_found' }, 404);
        if (attachment) {
          if (!['GET', 'POST'].includes(request.method)) return json({ error: 'method_not_allowed' }, 405);
          if (request.method === 'POST' && (request.headers.get('Origin') !== url.origin || request.headers.get('Sec-Fetch-Site') === 'cross-site' || request.headers.get('X-Cmail-Action') !== 'draft')) return json({ error: 'invalid_origin' }, 403);
          try { return await env.MAIL_QUEUE.getByName('primary').fetch(new Request('https://mail-queue.internal/drafts/attachments/' + attachment[1], request)); }
          catch { return json({ error: 'attachment_unavailable' }, 503); }
        }
        if (asset) {
          if (request.method !== 'GET') return json({ error: 'method_not_allowed' }, 405);
          return new Response(path.endsWith('.js') ? draftsScript : draftsStyles, { headers: { 'Content-Type': path.endsWith('.js') ? 'text/javascript; charset=utf-8' : 'text/css; charset=utf-8', 'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff' } });
        }
        let route, input;
        if (request.method === 'GET') {
          if (reply) { route = 'reply'; input = { id: reply[1] }; }
          else if (draft && !draft[2]) { route = 'get'; input = { id: draft[1] }; }
          else if (path === '/cmail/api/drafts') { route = 'list'; input = { mailbox: url.searchParams.get('mailbox') || '', query: url.searchParams.get('query') || '', cursor: url.searchParams.get('cursor') || '' }; }
          else return json({ error: 'method_not_allowed' }, 405);
        } else if (request.method === 'POST' && (path.endsWith('/save') || draft?.[2])) {
          if (request.headers.get('Origin') !== url.origin || request.headers.get('Sec-Fetch-Site') === 'cross-site' || request.headers.get('X-Cmail-Action') !== 'draft') return json({ error: 'invalid_origin' }, 403);
          if (request.headers.get('Content-Type')?.split(';')[0].trim().toLowerCase() !== 'application/json') return json({ error: 'invalid_draft' }, 400);
          try {
            const body = await request.text();
            if (body.length > 650000) return json({ error: 'invalid_draft' }, 413);
            const parsed = JSON.parse(body);
            if (!parsed || typeof parsed !== 'object') return json({ error: 'invalid_draft' }, 400);
            route = draft?.[2] || 'save';
            input = draft ? { id: draft[1], revision: parsed.revision } : { id: parsed.id, revision: parsed.revision, save_id: parsed.save_id, draft: parsed.draft };
          } catch { return json({ error: 'invalid_draft' }, 400); }
        } else return json({ error: 'method_not_allowed' }, 405);
        try { return await env.MAIL_QUEUE.getByName('primary').fetch('https://mail-queue.internal/drafts/' + route, { method: 'POST', body: JSON.stringify(input) }); }
        catch { return json({ error: 'draft_unavailable' }, 503); }
      }
      const response = await worker.fetch(request, env, context);
      if (env.CMAIL_DRAFTS_ENABLED === '1' && response.ok && (path === '/' || path === '/cmail/') && response.headers.get('Content-Type')?.includes('text/html')) {
        const html = (await response.text()).replace('<body>', '<body data-cmail-drafts="1">')
          .replace('<option value="inbox">Inbox</option>', '<option value="inbox">Inbox</option><option value="drafts">Drafts</option>')
          .replace('</head>', '<link rel="stylesheet" href="/cmail/drafts.css"><script src="/cmail/drafts.js" defer></script></head>');
        return new Response(html, response);
      }
      return response;
    },
  };
}
