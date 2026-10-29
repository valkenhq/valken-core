# Changelog

Notable changes, newest first. Versions are git tags; see *Contributing
and releases* in the README.

## 0.1.0

Initial public release: cluster connection and watch, resource store and
relationship graph, live tables, events, metrics, ingress traffic tailing,
the security inventory, and the one write path (`k8s::write`, dry run
first).

What never leaves the cluster or reaches a log: Secret values, env values,
spec-copy annotations and credential-plugin environments — see
`SECURITY.md` and `AGENTS.md`.
