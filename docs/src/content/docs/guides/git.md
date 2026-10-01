---
title: Preserve and Serve a Native Git View
description: Import native Git objects without translating their identity, then inspect, materialize, or serve an immutable ref view.
---

Casita preserves Git's native object identity and stores an immutable view of
selected refs. It is a retention and read path, not a Git authoring or push
service.

## 1. Import a repository

### CLI

```console
$ casita import ./project \
    --git-view upstream \
    --git-ref refs/heads/main
```

`SOURCE` may be a working tree or bare repository; Casita detects both from
their metadata. Without explicit `--git-ref` arguments, Casita selects local
branches and tags. When `--git-view` is omitted during auto-detection, Casita
uses the source directory's basename. Import verifies native Git objects,
publishes one immutable ref view, and sets the root `git/upstream`.

`--git-concurrency` bounds active object staging (default 16), and
`--git-max-buffered-bytes` bounds decoded source bytes held by staging futures
(default 67108864, or 64 MiB). Both must be positive. Set concurrency to 1 for
serial staging. An object larger than the byte budget runs alone. Gix caches,
delta-decoding workspace and payload-store buffers are outside this budget.
Source decoding remains synchronous; object verification and storage overlap.

### Rust

Enable Casita's `git` feature, then pass a local working tree or bare
repository and the selected refs. An empty `refs` list selects local branches
and tags. `max_cached_pack_bytes` defaults to 8 GiB and retains a verified
exact source pack for fast full clones; set it to zero when minimizing stored
bytes is more important than full-clone throughput.

`GitImport::with_concurrency` and `with_max_buffered_bytes` configure the same
limits using nonzero integers. `NativeGitImportOptions` also exposes
`concurrency` and `max_buffered_bytes` fields.

```toml
[dependencies]
casita = { git = "https://github.com/cachix/casita", default-features = false, features = ["git"] }
```

```rust
# async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use casita::{import::GitImport, Repository};

let repository = Repository::local("./cache").await?;
let outcome = repository
    .import(
        GitImport::new("./project", "upstream")
            .with_refs(["refs/heads/main"])?,
    )
    .await?;

println!("{} objects in {}", outcome.objects, outcome.view);
# Ok(())
# }
```

## 2. Inspect the view

```console
$ casita --repository ./cache git show upstream
```

The output includes the view key, object format, optional default ref, and
direct or symbolic refs. A view captures one exact ref state; importing again
creates a new immutable view and repoints the destination-owned root.

## 3. Materialize an exact tree

```console
$ casita git checkout \
    git.sha1.tree.v1:... ./tree
```

The target must be absent or empty. Gitlinks fail by default because Casita
does not silently fetch or materialize submodules; `--skip-gitlinks` creates
empty directories for them instead.

## 4. Offer read-only clones

With smart-HTTP support enabled, bind one immutable view:

```console
$ casita git serve upstream \
    --listen 127.0.0.1:9418
```

The printed URL supports read-only full and shallow fetches. The service does
not implement receive-pack, branch mutation, review, merge, or repository
administration. Put authentication, TLS, network exposure, and process
supervision around it as deployment policy requires.

See [Object Formats](../../reference/object-formats/) for Git namespaces and
the [CLI Reference](../../reference/cli/#native-git) for exact command forms.
