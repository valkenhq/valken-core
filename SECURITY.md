# Security policy

valken-core connects to Kubernetes clusters with the user's own kubeconfig
and runs their credential plugins, so a bug here can expose cluster
credentials or data. Please report anything in that category privately.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting: **Security → Report a
vulnerability** on this repository. Do not open a public issue.

Include what you found, how to reproduce it, and what an attacker could do
with it. You'll get an acknowledgement within a few days and a fix or a
plan before anything is disclosed.

## What counts

* Secret values, tokens, client keys or exec-plugin environments reaching
  logs, error messages, the UI or an export.
* Anything that writes to a cluster outside `k8s::write`, or a write that
  skips the dry-run / field-manager / precondition path.
* Running a binary other than the one the kubeconfig names, or with an
  environment it didn't ask for.

## Supported versions

Only the latest release is supported. Fixes are released as a new version
and noted in [CHANGELOG.md](CHANGELOG.md).
