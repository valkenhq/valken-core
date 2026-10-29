# Contributing

Thanks for taking a look. The short version: branch, change, `make ci`,
bump the version, open a PR.

## Setup

`rust-toolchain.toml` pins the toolchain; `rustup` picks it up on first
build. No cluster is needed for development.

```sh
make test-core   # engine-free tests, fast
make ci          # what CI runs: fmt, clippy (both feature sets), tests, docs
```

The `godot` feature (on by default) builds the three Godot nodes; everything
else is plain Rust and must keep building with `--no-default-features`.

## Live tests

`tests/*_live.rs` are `#[ignore]`d and need a kubeconfig in
`VALKEN_TEST_KUBECONFIG`; they never read `KUBECONFIG` or `~/.kube/config`.
Every write they make is a server-side dry run. Point them at a throwaway
cluster anyway.

```sh
VALKEN_TEST_KUBECONFIG=~/.kube/kind.yaml cargo test -- --ignored
```

## Rules of the crate

* **Read-only.** Only `k8s::write` changes a cluster. A new call that
  creates, patches or deletes anything belongs there, behind `Mode`.
* **Nothing sensitive in output.** Secret values, env values, tokens and
  exec-plugin environments must not reach logs, errors or the UI. Display
  kube errors through `k8s::error::describe`; log with `%`, never `?`, on
  anything that holds a resource.
* **I/O behind traits.** New cluster access goes through a seam
  (`ClusterSource`, `MetricsSource`, `LogSource`, `SecuritySource`) so the
  pipeline stays testable with the fakes in `tests/support`.
* **Tests with the change.** Pure logic gets a unit test next to it;
  pipeline behaviour gets an integration test over the fakes.

## AI agents

Using an AI coding agent (Claude Code, Codex, Cursor, Copilot, …) to write
or change code here is fine; `AGENTS.md` tells agents the rules. Two
conditions:

* **A human reviews everything.** Every line in a PR has been read and
  understood by the person opening it, and is reviewed by a maintainer like
  any other change. "The agent wrote it" is not an explanation in review.
* **The author carries the risk.** You are responsible for what you submit
  — correctness, licensing of anything it reproduced, and the security
  rules above — exactly as if you had typed it. If you can't vouch for a
  change, don't submit it.

Say in the PR that an agent was involved; it helps reviewers know where to
look harder.

## Pull requests

Every PR into `main` bumps `Cargo.toml` with `make bump-patch`,
`bump-minor` or `bump-major` (the "Version bumped" check enforces it); the
merge is tagged with that version. Label the PR `release:skip` for changes
that shouldn't release (docs, CI). Add a line to `CHANGELOG.md` under
*Unreleased*.
