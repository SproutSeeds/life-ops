# C-mail command changes

C-mail changes must also work on the website, using the same cloud state,
validation, limits, revisions and authorization. Follow
`../../docs/cmail-feature-parity.md` and `../../docs/cmail-agent/README.md`.
Update both adapters and their cross-interface tests in the same release.
The installed `cmail` and `lifeops cmail` routes both use
`src/life_ops/cmail_cloud.py`; verify both entrypoints. The legacy local mailbox
and direct provider paths remain retired.
