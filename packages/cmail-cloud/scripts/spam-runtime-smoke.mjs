// Native SQLite/workerd fixture with external connections disabled. Never sends.
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { bindings, primary } from '../test/reply-fixture.mjs';
import { spamPayload } from '../test/spam-fixture.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const serve = process.argv.includes('--serve'), root = fileURLToPath(new URL('../', import.meta.url));
const fetchMock = createFetchMock(); fetchMock.disableNetConnect();
const mf = new Miniflare({ name:'cmail-spam-fixture', host:'127.0.0.1', port:serve ? 9346 : 0,
  modules:true, modulesRoot:root, scriptPath:root+'test/spam-worker.mjs', compatibilityDate:'2026-03-27', fetchMock,
  bindings:{...bindings,CMAIL_FAST_READS:'1',CMAIL_SENT_ENABLED:'1',CMAIL_COMPOSE_ENABLED:'1',CMAIL_DRAFTS_ENABLED:'1',CMAIL_AGENT_ENABLED:'1'},
  durableObjects:{MAIL_QUEUE:{className:'MailQueue',useSQLite:true}},
});
try {
  const ns=await mf.getDurableObjectNamespace('MAIL_QUEUE'), stub=ns.get(ns.idFromName('primary'));
  const internal=async(path,body) => {const r=await stub.fetch('https://internal'+path,{method:'POST',body:JSON.stringify(body)}); assert.equal(r.status,200); return r.json();};
  const first=await internal('/enqueue',spamPayload(1,{attachment:true}));
  const safe=await internal('/enqueue',spamPayload(2,{from:'Friend <friend@example.test>'}));
  const own=await internal('/enqueue',spamPayload(3,{from:primary,direction:'outbound'}));
  const original=await internal('/archive/message',{id:first.id,raw:true});
  await internal('/archive/actions',{ids:[first.id],action:'spam'});
  const future=await internal('/enqueue',spamPayload(4));
  assert.equal((await internal('/archive/message',{id:future.id})).spam,true);
  assert.equal((await internal('/archive/list',{view:'spam'})).total_view,2);
  assert((await internal('/enqueue',spamPayload(4))).duplicate);
  const probe=await internal('/performance/probe',{parameters:{view:'spam',limit:20}});
  assert.equal(probe.raw_payload_loads,0); assert.equal(probe.list_calls,1); assert(probe.get_calls<=4);
  await internal('/archive/actions',{ids:[first.id,future.id],action:'not-spam'});
  assert.equal((await internal('/archive/list',{view:'spam'})).total_view,0);
  assert.deepEqual(await internal('/archive/message',{id:first.id,raw:true}),original);
  await internal('/archive/actions',{ids:[first.id,future.id,safe.id,own.id],action:'read'});
  console.log(JSON.stringify({passed:true,provider_calls:0,probe,checks:['future exact-sender filtering','replay deduplication','restore and unblock','original MIME unchanged','bounded folder reads']}));
  if(serve){console.log(JSON.stringify({browser_fixture_url:String(await mf.ready)+'cmail/',pid:process.pid}));await new Promise(resolve=>{process.on('SIGTERM',resolve);process.on('SIGINT',resolve);});}
} finally {await mf.dispose();}
