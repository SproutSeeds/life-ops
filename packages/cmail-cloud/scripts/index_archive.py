"""Build additive encrypted read indexes; originals and message state are untouched."""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
from life_ops import cloudflare_email, credentials

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--probe', action='store_true')
    parser.add_argument('--sent', action='store_true', help='Build/inspect the Sent conversation index')
    parser.add_argument('--receipt-dir', type=Path, default=Path('data/qa/cmail-performance-2026-10-18'))
    args = parser.parse_args()
    secret = credentials.resolve_secret(name='LIFE_OPS_MAIL_INGEST_SECRET')
    def call(operation, payload=None):
        area = 'sent' if args.sent and operation != 'probe' else 'performance'
        return cloudflare_email._cloudflare_worker_request_json(worker_public_url='https://life-ops-email-ingest.frg-life-ops.workers.dev', secret=secret, path='/api/mail/' + area + '/' + operation, payload=payload or {})
    args.receipt_dir.mkdir(parents=True, exist_ok=True)
    before = call('status')
    print(json.dumps({'before': before}), flush=True)
    batches = []
    if args.apply:
        for _ in range(10000):
            state = call('backfill', {'limit': 20})
            batches.append(state)
            if len(batches) % 10 == 0 or state['ready']:
                print(json.dumps(state), flush=True)
            (args.receipt_dir/('sent-index-build.json' if args.sent else 'index-build.json')).write_text(json.dumps({'before': before, 'batches': batches, 'last': state}, indent=2)+'\n')
            if state['ready']: break
        else: raise RuntimeError('Backfill did not converge')
    if args.probe:
        cases = [('list', {'mailbox': 'support@bindmysoul.com', 'limit': 50}),
                 ('conversation', {'id': 'mail_0000000000002472'}), ('message', {'id': 'mail_0000000000002472'})]
        if args.sent:
            cases = [('list', {'mailbox': 'support@bindmysoul.com', 'view': 'sent', 'limit': 50}), ('list', {'view': 'sent', 'limit': 50}), ('conversation', {'id': 'mail_0000000000002472'})]
        results = []
        for operation, parameters in cases:
            for legacy in ([False] if args.sent else [True, False]):
                result = call('probe', {'operation': operation, 'legacy': legacy, 'parameters': parameters})
                results.append(result)
                print(json.dumps(result), flush=True)
        (args.receipt_dir/('sent-storage-probes.json' if args.sent else 'storage-probes.json')).write_text(json.dumps({'checked_at':datetime.now(timezone.utc).isoformat(), 'results':results}, indent=2)+'\n')

if __name__ == '__main__': main()
