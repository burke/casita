<h1>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/public/brand/logo-dark.svg">
    <img src="docs/public/brand/logo.svg" alt="Casita" width="354" height="120">
  </picture>
</h1>

> [!WARNING]
> Casita is pre-release software. Public Rust and CLI interfaces may change;
> the S3 storage profile and advanced Rust APIs are experimental.

Casita stores source code and build artifacts in a verified, content-addressed
object repository written in Rust. Store filesystem trees, native Git objects,
IPLD blocks, and custom object graphs while preserving each format's identity.
They share deduplicated storage, named roots, synchronization, and garbage
collection.

Runs on Linux, macOS, and Windows.

## Features

- **Verified storage:** check object identities and links before publication,
  verify chunks on read, and authenticate file ranges with Bao proofs.
- **Deduplication:** share content across objects with BLAKE3 identities,
  FastCDC chunking, and zstd compression.
- **Named roots:** give graphs durable names, replace them atomically, and
  collect unreachable data while active readers retain what they need.
- **Synchronization:** copy graphs between local repositories or from SSH
  sources, with independent verification at the destination. An experimental
  S3 profile supports shared repositories across runners.
- **Import and export:** import filesystem trees, native Git objects, and OCI images
  (with `oci`) as layouts and optional merged filesystems. Restore files and
  exchange graphs through portable Casitar archives.
- **Rust library:** embed the repository in an application, with experimental
  APIs for custom object formats and storage backends.

The companion [`casita-fs`](crates/casita-fs/README.md) crate exposes filesystem
trees through Linux FUSE and macOS FSKit.

## Quick start

Rust 1.94.1 or newer is required. Build the CLI from a source checkout:

```console
$ git clone https://github.com/cachix/casita.git
$ cd casita
$ cargo install --path crates/casita
```

### Store and restore files

Initialize a workspace, then import source code under a named root:

```console
$ casita init
$ casita import ./crates/casita/src --root examples/source
$ casita root ls
```

Casita stores objects in its global data directory. The `.casita` marker
attaches this workspace to that storage and keeps its root names private to
the workspace. Import build artifacts the same way, using their output directory.

The import prints a directory object key. Replace `casita.directory.v1:...`
with that key to inspect or restore the tree:

```console
$ casita tree list casita.directory.v1:...
$ casita checkout casita.directory.v1:... ./restored
```

### Synchronize repositories

Use `casita sync` to copy graphs between repositories. Install with
`--features ssh` on both machines to synchronize from an SSH source, or
`--features s3` for the experimental S3 profile. See the
[sync guide](docs/src/content/docs/guides/sync.md) for remote sources,
path selection, and incremental transfers.

### Verify and collect

Audit repository integrity and preview garbage collection:

```console
$ casita fsck --dry-run
$ casita gc --dry-run
```

Named roots retain their reachable objects. Run `gc` without `--dry-run` to
collect unreachable data. See the
[operations guide](docs/src/content/docs/guides/operations.md) for maintenance,
repair, and diagnostics.

## Compatibility

Casita's Rust and CLI interfaces are still evolving. The standard local
repository uses a pinned pre-release Turso build with experimental
multi-process WAL. Review the
[backup and recovery guidance](docs/src/content/docs/reference/local-repository.md)
before production use.

Object identities and canonical encodings are described in the
[object formats reference](docs/src/content/docs/reference/object-formats.md).
Physical storage layouts have their own compatibility requirements. See the
[security policy](SECURITY.md) for verification guarantees, limitations, and
vulnerability reporting.

## Documentation

- [Quick start](docs/src/content/docs/getting-started.md): installation and first repository.
- [CLI reference](docs/src/content/docs/reference/cli.md): commands and options.
- [Rust library](docs/src/content/docs/library.md): embedding Casita in applications.
- [Cargo features](docs/src/content/docs/reference/cargo-features.md): optional integrations and build requirements.
- [Concepts](docs/src/content/docs/concepts/index.md): object model, storage, synchronization, and collection.
- [Benchmarks](benchmarks/README.md): reproducible workloads and correctness checks, with [published results](https://casita.rs/benchmarks/).
- [Release process](RELEASING.md): validation and distribution requirements.

## License

Apache-2.0. See [LICENSE](LICENSE).
