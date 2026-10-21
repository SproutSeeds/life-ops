// Local-only fixture. No test routes or authentication bypass enter src/.
import worker, { MailQueue } from './compose-worker.mjs';
import { spamPayload } from './spam-fixture.mjs';
export { MailQueue };
const counts = {}; let failNext = false, sequence = 10;
export default {
  async fetch(request, env, context) {
    const path = new URL(request.url).pathname;
    if (path === '/fixture/spam-metrics') return Response.json(counts);
    if (path === '/fixture/spam-fail' && request.method === 'POST') { failNext = true; return Response.json({ok:true}); }
    if (path === '/fixture/spam-deliver' && request.method === 'POST') return env.MAIL_QUEUE.getByName('primary').fetch('https://internal/enqueue', {
      method: 'POST', body: JSON.stringify(spamPayload(++sequence, { from: 'New offer <OFFERS@example.test>', mailbox: 'support@bindmysoul.com' })),
    });
    if (path === '/fixture/spam-narrow') return new Response('<!doctype html><html><body style="margin:0;background:#27362c"><iframe title="Narrow Spam reader" src="/cmail/?fixture_embed=1&view=spam" style="border:0;width:390px;height:844px"></iframe></body></html>', {headers:{'Content-Type':'text/html'}});
    counts[request.method + ' ' + path] = (counts[request.method + ' ' + path] || 0) + 1;
    if (failNext && path === '/cmail/api/messages/actions') { failNext = false; return Response.json({error:'synthetic_failure'},{status:503}); }
    return worker.fetch(request, env, context);
  },
};
