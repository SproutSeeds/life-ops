// Local Miniflare fixture only. This file is outside the deployed src directory.
import { createWorker, withResend, MailQueue } from '../src/index.mjs';
import { withReplies } from '../src/reply-http.mjs';
const localAccess = async () => ({ ok: true, email: 'fixture@example.test' });
export { MailQueue };
export default withReplies(withResend(createWorker(localAccess)), localAccess);
