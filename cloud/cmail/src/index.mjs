import legacyWorker from './legacy-worker.mjs';
import { authorize } from './access.mjs';
import { MailQueue } from './archive.mjs';
import page from '../web/index.html';
import styles from '../web/style.css';
import client from '../web/app.js?raw';
import icon from '../web/icon-192.png';
import largeIcon from '../web/icon-512.png';

export { MailQueue };
const PREFIX = '/cmail';
const PUBLIC_HOSTNAME = 'cmail.frg.earth';
const securityHeaders = {
  'Cache-Control': 'private, no-store',
  'X-Content-Type-Options': 'nosniff',
  'Referrer-Policy': 'no-referrer',
  'X-Frame-Options': 'DENY',
  'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; font-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
};

function response(body, type = 'application/json; charset=utf-8', status = 200, extra = {}) {
  return new Response(body, { status, headers: { ...securityHeaders, 'Content-Type': type, ...extra } });
}
function json(value, status = 200) { return response(JSON.stringify(value), undefined, status); }
function bytes(base64) { return Uint8Array.from(atob(base64), (character) => character.charCodeAt(0)); }

export function createWorker(checkAccess = authorize) {
  return {
    email: legacyWorker.email,
    async fetch(request, env, context) {
      const url = new URL(request.url);
      const publicHostname = url.hostname === PUBLIC_HOSTNAME;
      const publicRoot = publicHostname && url.pathname === '/';
      if (!publicRoot && url.pathname !== PREFIX && !url.pathname.startsWith(`${PREFIX}/`)) {
        return legacyWorker.fetch(request, env, context);
      }
      const access = await checkAccess(request, env);
      if (!access.ok) return json({ error: access.error }, access.status);
      const readStatusMatch = url.pathname.match(/^\/cmail\/api\/messages\/(mail_\d{16})\/read-status$/);
      if (readStatusMatch) {
        if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
        if (request.headers.get('Origin') !== url.origin || request.headers.get('Sec-Fetch-Site') === 'cross-site') {
          return json({ error: 'invalid_origin' }, 403);
        }
        if (request.headers.get('Content-Type')?.split(';')[0].trim().toLowerCase() !== 'application/json') {
          return json({ error: 'invalid_request' }, 400);
        }
        let body;
        try {
          const text = await request.text();
          if (text.length > 128) return json({ error: 'invalid_request' }, 400);
          body = JSON.parse(text);
        } catch { return json({ error: 'invalid_request' }, 400); }
        if (typeof body?.read !== 'boolean') return json({ error: 'invalid_request' }, 400);
        try {
          const result = await env.MAIL_QUEUE.getByName('primary').fetch('https://mail-queue.internal/archive/read-status', {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ id: readStatusMatch[1], read: body.read }),
          });
          return json(await result.json(), result.status);
        } catch { return json({ error: 'read_status_unavailable' }, 503); }
      }
      if (request.method !== 'GET') return json({ error: 'read_only' }, 405);
      if (url.pathname === PREFIX) return response('', undefined, 302, { Location: `${PREFIX}/` });
      if (publicRoot || url.pathname === `${PREFIX}/`) {
        return response(publicHostname ? page.replace('href="/cmail/"', 'href="/"') : page, 'text/html; charset=utf-8');
      }
      if (url.pathname === `${PREFIX}/style.css`) return response(styles, 'text/css; charset=utf-8');
      if (url.pathname === `${PREFIX}/app.js`) return response(client, 'text/javascript; charset=utf-8');
      if (url.pathname === `${PREFIX}/icon-192.png`) return response(bytes(icon), 'image/png');
      if (url.pathname === `${PREFIX}/icon-512.png`) return response(bytes(largeIcon), 'image/png');
      if (url.pathname === `${PREFIX}/manifest.webmanifest`) {
        const home = publicHostname ? '/' : `${PREFIX}/`;
        return response(JSON.stringify({
          id: home, name: 'C-mail', short_name: 'C-mail',
          description: 'Your connected mail, available wherever you are.',
          start_url: home, scope: home, display: 'standalone',
          background_color: '#0d1411', theme_color: '#0d1411',
          icons: [192, 512].map((size) => ({ src: `${PREFIX}/icon-${size}.png`, sizes: `${size}x${size}`, type: 'image/png', purpose: 'any' })),
        }), 'application/manifest+json');
      }
      if (url.pathname === `${PREFIX}/api/session`) {
        const mailboxes = [...new Set(String(env.CMAIL_MAILBOXES || '').split(/[,\n]/)
          .map((value) => value.trim().toLowerCase())
          .filter((value) => value.length <= 254 && /^[^\s<>@]+@[^\s<>@]+\.[^\s<>@]+$/.test(value)))];
        return json({ email: access.email, mailboxes, read_only: true });
      }
      let path;
      let parameters;
      let download = false;
      if (url.pathname === `${PREFIX}/api/messages`) {
        path = '/archive/list';
        parameters = Object.fromEntries(['query', 'mailbox', 'cursor', 'after', 'before', 'limit'].map((key) => [key, url.searchParams.get(key) || '']));
      } else {
        const match = url.pathname.match(/^\/cmail\/api\/messages\/(mail_\d{16})(?:\/(raw|attachments\/\d+))?$/);
        if (!match) return json({ error: 'not_found' }, 404);
        path = '/archive/message';
        parameters = { id: match[1] };
        if (match[2] === 'raw') { parameters.raw = true; download = true; }
        if (match[2]?.startsWith('attachments/')) {
          parameters.attachment = Number(match[2].split('/')[1]); download = true;
        }
      }
      try {
        const queue = env.MAIL_QUEUE.getByName('primary');
        const result = await queue.fetch(`https://mail-queue.internal${path}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(parameters),
        });
        const payload = await result.json();
        if (!result.ok) return json(payload || { error: 'not_found' }, result.status);
        if (download) {
          const filename = String(payload.filename || 'message').replace(/[\r\n"\\/\u0000-\u001f]/g, '_');
          return response(bytes(payload.content_base64), 'application/octet-stream', 200, {
            'Content-Disposition': `attachment; filename="${filename.replace(/[^\x20-\x7e]/g, '_')}"; filename*=UTF-8''${encodeURIComponent(filename)}`,
          });
        }
        return json(payload);
      } catch {
        return json({ error: 'mail_temporarily_unavailable' }, 503);
      }
    },
  };
}

export default createWorker();
