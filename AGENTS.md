# Working in valken-core with an AI agent

This file is for coding agents (Claude Code, Codex, Cursor, Copilot, …) and
the people driving them. Humans: read CONTRIBUTING.md too.

## What this crate is

The Kubernetes connectivity library for [Valken](https://valken.io): kube 3
+ tokio, plain Rust except three Godot nodes behind the `godot` feature.
Module map and architecture are in `README.md` and the `//!` docs at the top
of each module — read those before changing a module.

## Non-negotiables

1. **Read-only.** Only `src/k8s/write.rs` may create, patch or delete
   anything in a cluster, and every path there takes a `Mode` so it can dry
   run. The one accepted POST elsewhere is the `SelfSubjectAccessReview` in
   `kubeconfig/probe.rs`. Do not add others.
2. **Nothing sensitive in output.** Secret values, env values, tokens,
   kubeconfig auth material and exec-plugin environments must not reach
   logs, error strings, the UI or exports.
   - Display kube errors through `k8s::error::describe` /
     `describe_watch`, never `e.to_string()` on a raw `kube::Error`.
   - Log resources with `%name`, never `?resource`; `Watched` and
     `KubeResource` `Debug` print full specs.
   - Secret reads go through `objects::get_object_with(.., SecretValues)`
     or `SecuritySource::pull_credentials`; `security::redact` for exports.
3. **I/O behind traits.** Cluster access goes through `ClusterSource`,
   `MetricsSource`, `LogSource` or `SecuritySource`, so the pipeline stays
   testable with the fakes in `tests/support/mod.rs`.
4. **One copy of each object.** `Watched` / `KubeResource` hold `Arc<..>`
   shared with the reflector store. Don't clone a Pod out of an Arc; don't
   keep `managedFields`.
5. **Both feature sets build.** `--no-default-features` must stay engine-free.

## Commands

```sh
make test-core   # fast lane, engine-free tests
make ci          # fmt-check, clippy -D warnings (both feature sets), tests, docs — must pass
make bump-patch  # every PR bumps the version (or is labelled release:skip)
```

Never run the `*_live.rs` tests against a cluster you don't own; they need
`VALKEN_TEST_KUBECONFIG` and are `#[ignore]`d for a reason.

## How to change things

- Match the surrounding style: `rustfmt` defaults, typed `thiserror` errors,
  module `//!` docs that say *why*, comments only where the code can't say it.
- Pure logic gets a unit test next to it; pipeline behaviour gets an
  integration test over the fakes. A regression test must fail without the
  fix — check it does.
- Don't widen the public API casually: re-exports are explicit lists, new
  items default to `pub(crate)`, aliases kept for compatibility are
  `#[deprecated]`.
- Add a line to `CHANGELOG.md` under *Unreleased* for anything user-visible.
- Don't commit, push, tag or release unless asked. Commits are GPG-signed by
  the human.

## Review

Agent-written code is welcome and is reviewed exactly like human-written
code. The person opening the PR has read every line, can explain it, and is
responsible for it — see CONTRIBUTING.md. If you (the agent) are unsure
whether a change crosses one of the non-negotiables above, stop and ask.
