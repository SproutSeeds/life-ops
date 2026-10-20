// Isolated synthetic fixture; this file is never uploaded by deploy.py.
import worker, { MailQueue } from './reply-worker.mjs';
export { MailQueue };
const metrics = { contacts: 0, compose_sends: 0, draft_saves: 0, draft_sends: 0, draft_lists: 0 };
export default {
  async fetch(request, env, context) {
    const path = new URL(request.url).pathname;
    if (path === '/fixture/metrics') return Response.json(metrics);
    if (path === '/cmail/api/contacts') metrics.contacts++;
    if (path === '/cmail/api/compose' && request.method === 'POST') metrics.compose_sends++;
    if (path === '/cmail/api/drafts/save') metrics.draft_saves++;
    if (/\/cmail\/api\/drafts\/draft_[a-f0-9-]+\/send$/.test(path)) metrics.draft_sends++;
    if (path === '/cmail/api/drafts') metrics.draft_lists++;
    return worker.fetch(request, env, context);
  },
};
