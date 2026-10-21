// Isolated browser fixture only; deploy.py uploads src/*.mjs, never this file.
import worker, { MailQueue } from './compose-worker.mjs';
export { MailQueue };
const requests = {};
export default {
  async fetch(request, env, context) {
    const path = new URL(request.url).pathname;
    if (path === '/fixture/reader-metrics') return Response.json(requests);
    if (path === '/fixture/reader-narrow') return new Response('<!doctype html><html><body style="margin:0;background:#27362c"><iframe title="Narrow C-mail reader" src="/cmail/?fixture_embed=1#mail_0000000000000001" style="border:0;width:390px;height:844px"></iframe></body></html>', { headers: { 'Content-Type': 'text/html' } });
    requests[request.method + ' ' + path] = (requests[request.method + ' ' + path] || 0) + 1;
    return worker.fetch(request, env, context);
  },
};
