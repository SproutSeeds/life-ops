import { authorize } from './production-base.mjs';
import { replyScript, replyStyles } from './reply-ui.mjs';
import { appScript } from './web-app.mjs';
import { composeScript, composeStyles } from './compose-ui.mjs';
import { signatureLogo } from './signature.mjs';
import { sendingIdentities } from './reply.mjs';
import { messageStyles } from './message-format.mjs';

const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff' } });

export function withReplies(worker, checkAccess = authorize) {
  return {
    ...worker,
    async fetch(request, env, context) {
      const url = new URL(request.url);
      const message = url.pathname.match(/^\/cmail\/api\/messages\/(mail_\d{16})\/reply$/);
      const status = url.pathname.match(/^\/cmail\/api\/replies\/([a-f0-9-]{36})$/);
      const compose = url.pathname === '/cmail/api/compose';
      const contacts = url.pathname === '/cmail/api/contacts';
      const composeAsset = ['/cmail/compose.js', '/cmail/compose.css'].includes(url.pathname);
      const asset = composeAsset || ['/cmail/reply.js', '/cmail/reply.css'].includes(url.pathname);
      const conversation = url.pathname.match(/^\/cmail\/api\/messages\/(mail_\d{16})\/conversation$/);
      if (conversation || env.CMAIL_FAST_READS === '1' && url.pathname === '/cmail/app.js') {
        const access = await checkAccess(request, env);
        if (!access.ok) return json({ error: access.error }, access.status);
        if (request.method !== 'GET') return json({ error: 'method_not_allowed' }, 405);
        if (!conversation) return new Response(appScript, { headers: { 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff' } });
        return env.MAIL_QUEUE.getByName('primary').fetch('https://mail-queue.internal/archive/conversation', { method: 'POST', body: JSON.stringify({ id: conversation[1] }) });
      }
      if (message || status || asset || compose || contacts) {
        const access = await checkAccess(request, env);
        if (!access.ok) return json({ error: access.error }, access.status);
        if ((compose || contacts || composeAsset) && env.CMAIL_COMPOSE_ENABLED !== '1') return json({ error: 'not_found' }, 404);
        if (asset) {
          if (request.method !== 'GET') return json({ error: 'method_not_allowed' }, 405);
          return new Response(url.pathname.endsWith('.js') ? composeAsset ? composeScript : replyScript : composeAsset ? composeStyles : replyStyles + messageStyles, { headers: {
            'Content-Type': url.pathname.endsWith('.js') ? 'text/javascript; charset=utf-8' : 'text/css; charset=utf-8',
            'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff',
          } });
        }
        let path, input;
        if (status && request.method === 'GET') { path = '/reply/status'; input = { request_id: status[1] }; }
        else if (message && request.method === 'GET') { path = '/reply/prepare'; input = { id: message[1] }; }
        else if (compose && request.method === 'GET') { path = '/compose/prepare'; input = {}; }
        else if (contacts && request.method === 'GET') {
          path = '/contacts/list'; input = { cursor: url.searchParams.get('cursor') || '', ...(url.searchParams.has('revision') ? { revision: Number(url.searchParams.get('revision')) } : {}) };
        }
        else if ((message || compose) && request.method === 'POST') {
          if (request.headers.get('Origin') !== url.origin || request.headers.get('Sec-Fetch-Site') === 'cross-site' || request.headers.get('X-Cmail-Action') !== (compose ? 'compose' : 'reply')) return json({ error: 'invalid_origin' }, 403);
          if (request.headers.get('Content-Type')?.split(';')[0].trim().toLowerCase() !== 'application/json') return json({ error: 'invalid_reply' }, 400);
          try {
            const body = await request.text();
            if (body.length > 650000) return json({ error: 'invalid_reply' }, 413);
            const parsed = JSON.parse(body);
            input = compose ? { request_id: parsed.request_id, sender: parsed.sender, to: parsed.to, subject: parsed.subject, text: parsed.text } : { id: message[1], request_id: parsed.request_id, text: parsed.text };
            if (parsed.attachments !== undefined) input.attachments = parsed.attachments;
          } catch { return json({ error: 'invalid_reply' }, 400); }
          path = compose ? '/compose/send' : '/reply/send';
        } else return json({ error: 'method_not_allowed' }, 405);
        try {
          return await env.MAIL_QUEUE.getByName('primary').fetch('https://mail-queue.internal' + path, {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(input),
          });
        } catch { return json({ error: 'reply_unavailable' }, 503); }
      }
      const response = await worker.fetch(request, env, context);
      if (!response.ok) return response;
      if ((url.pathname === '/' || url.pathname === '/cmail/') && response.headers.get('Content-Type')?.includes('text/html')) {
        let html = (await response.text()).replace('</head>', '<link rel="stylesheet" href="/cmail/reply.css"><script src="/cmail/reply.js" defer></script></head>');
        if (env.CMAIL_COMPOSE_ENABLED === '1') html = html.replace('</head>', '<link rel="stylesheet" href="/cmail/compose.css"><script src="/cmail/compose.js" defer></script></head>');
        if (env.CMAIL_SENT_ENABLED === '1') html = html.replace('<option value="archive">Archive</option>', '<option value="sent">Sent</option><option value="archive">Archive</option>');
        if (env.CMAIL_FAST_READS === '1') html = html.replace('<option value="all">All mail</option>', '<option value="spam">Spam</option><option value="all">All mail</option>');
        const headers = new Headers(response.headers);
        const logos = [...new Set(Object.values(sendingIdentities(env)).map((item) => signatureLogo(item)?.url).filter(Boolean))];
        if (logos.length) headers.set('Content-Security-Policy', headers.get('Content-Security-Policy').replace("img-src 'self'", "img-src 'self' " + logos.join(' ')));
        return new Response(html, { status: response.status, headers });
      }
      if (url.pathname === '/cmail/api/session') {
        return json({ ...await response.json(), read_only: false, reply_enabled: true, sent_enabled: env.CMAIL_SENT_ENABLED === '1', compose_enabled: env.CMAIL_COMPOSE_ENABLED === '1', spam_enabled: env.CMAIL_FAST_READS === '1' });
      }
      return response;
    },
  };
}
