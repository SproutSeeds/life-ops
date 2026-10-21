# Cloud C-mail Spam plan

Implement and deploy reversible sender-based Spam in the canonical cloud mailbox.

- Add Spam to the existing folder selector, with newest-first indexed pagination
  and search. Keep the existing reader, original content and downloads.
- Add Mark as spam to the reader and bulk Actions. Move selected messages out of
  Inbox/Archive into Spam and persist an exact From-address rule across inboxes.
  Address matching ignores case; it does not match domains, names or Reply-To.
- Future inbound delivery checks the rule inside the same storage transaction as
  ingestion and folder indexing. Outbound mail and owned addresses are protected.
- Not spam restores selected messages to Inbox and removes their sender rules.
  Other previously caught messages remain in Spam until explicitly restored.
  Normal Archive keeps spam quarantined; ordinary unarchive requires Not spam.
- All mail remains the complete archive, with Spam visibly labeled. Sent and
  conversation history retain their messages. No deletion, unsubscribe, sending,
  provider rejection or changes to explicit Life Ops obligations are introduced.
- Expose the same actions and Spam search through the canonical cloud CLI; retire
  misleading local Spam documentation. Legacy local preferences are historical
  and are not imported or activated as new cloud rules by this release.
- Verify exact matching, future delivery, repeat actions, concurrency, restart,
  counters, body preservation, authorization, bounded reads and failure recovery.
  Inspect desktop/narrow browser flows and Back/Escape. Deploy with a version
  guard, verify live source/authentication and inspect Spam in production without
  classifying real correspondence as test data.

Use the frontend-navigation skill for existing reader and folder navigation.
Keep unrelated classified workspace changes outside the scoped commit.
