// Isolated synthetic fixture; this file is never uploaded by deploy.py.
import worker, { MailQueue } from './reply-worker.mjs';
export { MailQueue };
const metrics = { contacts: 0, compose_sends: 0, draft_saves: 0, draft_sends: 0, draft_lists: 0, draft_reads: 0 };
let failSaves = 0;
export default {
  async fetch(request, env, context) {
    const path = new URL(request.url).pathname;
    if (path === '/fixture/metrics') return Response.json(metrics);
    if (path === '/fixture/control' && request.method === 'POST') {
      failSaves = Number((await request.json()).fail_saves || 0);
      return Response.json({ fail_saves: failSaves });
    }
    if (path === '/fixture/narrow') return new Response('<!doctype html><html><body style="margin:0;background:#27362c"><iframe title="Mobile C-mail" src="/cmail/?view=drafts&fixture_embed=1" style="border:0;width:390px;height:844px"></iframe></body></html>', { headers: { 'Content-Type': 'text/html' } });
    if (path === '/cmail/api/contacts') metrics.contacts++;
    if (path === '/cmail/api/compose' && request.method === 'POST') metrics.compose_sends++;
    if (path === '/cmail/api/drafts/save') {
      metrics.draft_saves++;
      if (failSaves > 0) { failSaves--; return Response.json({ error: 'draft_unavailable' }, { status: 503 }); }
    }
    if (/\/cmail\/api\/drafts\/draft_[a-f0-9-]+$/.test(path) && request.method === 'GET') metrics.draft_reads++;
    if (/\/cmail\/api\/drafts\/draft_[a-f0-9-]+\/send$/.test(path)) metrics.draft_sends++;
    if (path === '/cmail/api/drafts') metrics.draft_lists++;
    const response = await worker.fetch(request, env, context);
    // Only this synthetic local fixture permits a same-origin iframe so the
    // real responsive CSS can be inspected in a 390px-wide browsing context.
    if (new URL(request.url).searchParams.get('fixture_embed') === '1') {
      const headers = new Headers(response.headers);
      headers.delete('X-Frame-Options');
      headers.set('Content-Security-Policy', headers.get('Content-Security-Policy').replace("frame-ancestors 'none'", "frame-ancestors 'self'"));
      return new Response(response.body, { status: response.status, headers });
    }
    return response;
  },
};
