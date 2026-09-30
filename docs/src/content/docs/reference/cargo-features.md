---
title: Cargo Features
description: Compile-time feature flags, implications, and supported Casita capability sets.
---

Casita separates portable data-model and format code from the native repository
profile and optional frontends.

## Feature table

| Feature | Implies | Adds |
|---|---|---|
| `native` | — | Tokio-backed repository workflows, persistent and memory backends, collection, transfer, filesystem I/O, and native Git view operations |
| `experimental` | — | Public `casita::experimental` namespace for backend, format, tuning, and protocol APIs |
| `cli` | `native`, `experimental` | The `casita` command-line binary, including the local JSON-RPC/NDJSON service (`casita ipc`) |
| `git` | `native` | Import from a local working tree or bare Git repository through `gix` |
| `oci` | `native` | Stream registry images into OCI image layouts and optional merged filesystems through `oci-client` |
| `git-fetch` | `native` | Experimental read-only Git fetch planning and pack generation |
| `git-http` | `git`, `git-fetch` | Read-only Git smart-HTTP serving and Tokio networking |
| `ssh` | `native` | Authenticated transfer sources through the system OpenSSH client |
| `s3` | `native` | Experimental shared S3 storage profile and `Repository::s3` |
| `fuzzing` | `native`, `experimental` | Internal parser adapters for fuzz harnesses |

The default feature set is `cli`, which includes `native` and `experimental`.
An ordinary `cargo build` builds the `casita` binary. Library consumers that
do not need the CLI can use `default-features = false, features = ["native"]`.

Library consumers using `default-features = false` also enable `experimental`
to access service APIs such as Git fetch, HTTP, and SSH. Optional features
alone do not expose backend or protocol types at the crate root.

## Common configurations

Casita has not published a tagged release. The examples below follow current
development; replace `branch = "main"` with `rev = "..."` for reproducible
evaluation.

```toml
# Standard library use with local repositories, without the CLI dependencies.
casita = {
  git = "https://github.com/cachix/casita",
  branch = "main",
  default-features = false,
  features = ["native"],
}

# Portable identities and filesystem data types without native I/O.
casita = {
  git = "https://github.com/cachix/casita",
  branch = "main",
  default-features = false,
}

# Local native Git import.
casita = {
  git = "https://github.com/cachix/casita",
  branch = "main",
  default-features = false,
  features = ["git"],
}

# OCI registry image import.
casita = {
  git = "https://github.com/cachix/casita",
  branch = "main",
  default-features = false,
  features = ["oci"],
}

# Shared S3 storage through the application API; the storage profile is experimental.
casita = {
  git = "https://github.com/cachix/casita",
  branch = "main",
  default-features = false,
  features = ["s3"],
}

# Read-only native Git serving; `git` is implied.
casita = {
  git = "https://github.com/cachix/casita",
  branch = "main",
  default-features = false,
  features = ["git-http", "experimental"],
}

# Fetch planning and pack generation without the HTTP adapter.
casita = {
  git = "https://github.com/cachix/casita",
  branch = "main",
  default-features = false,
  features = ["git-fetch", "experimental"],
}
```

`Repository::s3` does not require the `experimental` feature. Custom backend
composition and the generic repository require `experimental`, including when
using local storage.

Install CLI combinations from a source checkout with:

```console
$ cargo install --path crates/casita
$ cargo install --path crates/casita --features ssh
$ cargo install --path crates/casita --features git
$ cargo install --path crates/casita --features oci
$ cargo install --path crates/casita --features git-http
```

## What remains without `native`

With `default-features = false`, the crate still provides:

- digests, typed IDs, object keys, records, roots, and revisions;
- canonical filesystem directory and path data types, including encoding and decoding.

Adding `experimental` also exposes `ObjectFormat`, `FormatRegistry`, portable
payload verification, IPLD formats, native Git object/view models, and Casitar
framing. Implementation modules remain private in every configuration.

Persistent stores, repository orchestration, streaming Casitar adapters,
filesystem workflows, transfer execution, and service adapters require
`native`. Native operations emit `tracing` spans and events but never install a
global subscriber, leaving filtering and collection to the embedding
application. `cli` adds the compact/JSON subscriber used by `--log-filter`,
`--log-format`, and `RUST_LOG`; its default filter is `off`.

## CLI feature behavior

The CLI command tree always shows all Git subcommands. Their runtime feature
requirements differ:

| Operation | Required build features |
|---|---|
| General CLI, Git view inspection, Git tree checkout | `cli` |
| `import -i git` | `cli,git` |
| `git serve` | `cli,git-http` |
| Local-to-local `sync` | `cli` |
| SSH-source `sync` and the remote source process | `cli,ssh` on both machines |

Calling `import -i git` or `git serve` from a binary without its required feature
returns an explicit error rather than hiding the command from help output.

## Toolchain and generated documentation

The current minimum supported Rust version is 1.94.1 and the crate uses Rust
edition 2024. `docs.rs` metadata enables every feature so feature-gated items
appear together. Reproduce that API surface locally with:

```console
$ cargo doc --all-features --no-deps
```
