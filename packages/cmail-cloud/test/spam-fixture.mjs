export function spamPayload(n, { from = 'Offers <offers@example.test>', mailbox = 'cody@frg.earth', direction = 'inbound', replyTo = '', attachment = false } = {}) {
  const text = attachment ? 'Content-Type: multipart/mixed; boundary=sample\r\n\r\n--sample\r\nContent-Type: text/plain\r\n\r\nPreserve this body.\r\n--sample\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename="proof.txt"\r\n\r\nPreserve this attachment.\r\n--sample--' : '\r\nPreserve this body.';
  const raw = `From: ${from}\r\nTo: ${mailbox}\r\nReply-To: ${replyTo}\r\nSubject: Spam fixture ${n}\r\nMessage-ID: <spam-${n}@example.test>\r\nDate: Wed, 21 Oct 2026 15:00:00 +0000\r\n${text}`;
  return { envelope_to: mailbox, direction, received_at: new Date(Date.UTC(2026, 9, 21, 15, n)).toISOString(), raw_base64: btoa(raw) };
}
