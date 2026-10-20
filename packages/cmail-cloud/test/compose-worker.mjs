// Isolated synthetic fixture; this file is never uploaded by deploy.py.
import worker, { MailQueue } from './reply-worker.mjs';
export { MailQueue };
const metrics = { contacts: 0, compose_sends: 0 };
export default {
  async fetch(request, env, context) {
    const path = new URL(request.url).pathname;
    if (path === '/fixture/metrics') return Response.json(metrics);
    if (path === '/cmail/api/contacts') metrics.contacts++;
    if (path === '/cmail/api/compose' && request.method === 'POST') metrics.compose_sends++;
    return worker.fetch(request, env, context);
  },
};
