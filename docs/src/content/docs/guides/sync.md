---
title: Synchronize Repositories
description: Copy objects, roots, or one filesystem path into a verified destination.
---

Use `casita sync` to copy selected data into another Casita repository. The
source holds one stable revision; the destination verifies incoming objects.
Choose a named root to keep a complete graph under the same name, or an object
key to copy a graph without naming it.

## Choose what to copy

**A named root** copies its full closure and installs the same name at the
destination:

```console
$ casita sync --from ./cache --to ./mirror --root projects/demo
```

**An object key** copies its forward closure by default. Repeat `--object` for
more keys. `--shallow` copies only the selected objects' records and payloads;
it never makes a root transfer shallow.

```console
$ casita sync --from ./cache --to ./mirror \
    --object casita.directory.v1:...
$ casita sync --from ./cache --to ./mirror \
    --object casita.blob.v1:... --shallow
```

**One path below a filesystem root** copies just that file or directory
closure. `--path` requires exactly one `--root` and cannot be combined with
`--object` or `--shallow`:

```console
$ casita sync --from ./cache --to ./mirror \
    --root projects/demo --path lib/python3.12 \
    --destination-root partial/python3.12
```

Casita verifies the path's ancestor directories but does not store them at the
destination. `--destination-root` gives the selected closure a durable name.
Without it, the copied objects are unrooted and may be collected. A symlink
encountered as the selected path is reported, but has no standalone object key
to copy or root.

## Select endpoints

| Endpoint | Source | Destination | Requirement |
| --- | --- | --- | --- |
| Local repository path | Yes | Yes | Standard CLI |
| `s3://BUCKET/PREFIX` | Yes | Yes | `s3` feature |
| `ssh://[user@]host[:port]/absolute/path` | Yes | No | `ssh` feature on both machines and remote `casita` on `PATH` |

For an SSH source, install the binary with SSH support on both machines:

```console
$ cargo install --path crates/casita --features ssh
$ casita sync \
    --from ssh://alice@example.com/var/lib/casita \
    --to ./mirror --root projects/demo
```

OpenSSH handles authentication, host-key checks, and proxy settings. Casita
still verifies the received objects locally. `--writer NAME` sets the
diagnostic WAL writer name when an S3 endpoint is involved.

Casita passes `ConnectTimeout=30`, `ServerAliveInterval=15`, and
`ServerAliveCountMax=3` to `ssh`, so an unreachable host fails within 30
seconds, and a host or network that stops answering mid-transfer fails the
sync within about 45 seconds instead of blocking it. These command-line
options take precedence over the same settings in `ssh_config`.

## Read payloads from another repository

Use `--from-blobs` when roots and object records live at one endpoint but the
required payloads are available from another:

```console
$ casita sync \
    --from ssh://alice@example.com/var/lib/casita \
    --from-blobs s3://casita-mirror/releases \
    --to ./mirror --root projects/demo
```

This example needs the `cli`, `ssh`, and `s3` features. The payload source must
be a Casita repository, not a bucket of arbitrary files. Both source sessions
stay retained during the transfer; their revisions may differ. The destination
checks payloads against the records from `--from`. A missing payload fails the
transfer without falling back to `--from`. An SSH payload source also needs
the corresponding object records to serve payloads by object key. If
`--from-blobs` is omitted, `--from` supplies both records and payloads.

## Repeat or check a transfer

A failed transfer may leave verified objects in the destination, but requested
roots move only after their complete closures verify. Retry the same command
to reuse work already done. Sync never removes destination data.

Add `--incremental` for faster repeated transfers when reusing a verified
destination closure is sufficient. The default examines the entire selected
source closure, including descendants of objects already at the destination.
With `--incremental`, those reused descendants are not audited at the source.

To inspect a copied root and run an integrity check:

```console
$ casita --repository ./mirror root ls projects/demo
$ casita --repository ./mirror fsck
```

See [Sync](../../concepts/sync/) for the transfer guarantees and the
[CLI Reference](../../reference/cli/#synchronization) for every option.
