# C-mail feature parity

For every C-mail feature or behavior change, implement the website and agent CLI
paths together. Use the same cloud store, validation, limits, revision checks
and send receipts. Neither surface may ship a mail capability missing from the
other. Read `../../docs/cmail-feature-parity.md` and the canonical agent guide.

Update the capability matrix and cross-interface regression coverage with the
change. `scripts/check-parity.py` is a mandatory production deployment gate.
Verify the installed `cmail`, `lifeops cmail`, and authenticated production
website after deployment. Use synthetic unsent drafts for live checks; test
sending only against the network-isolated fake provider unless Cody separately
authorizes a specific real message.
