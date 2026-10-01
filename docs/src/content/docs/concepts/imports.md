---
title: Imports
description: How Casita turns external data into verified objects and a named root.
---

An import reads an external source, verifies the resulting objects, and publishes
them in a repository. A named root keeps the finished graph available. Importers
share the repository's object records, roots, and collection rules; each format
does not need its own storage system.

## Publication and failure

Importers stage payloads in a mutation session, which protects those bytes while
the import runs. Each object is checked against its format before its record is
published. Large imports can publish intermediate batches without a root, then
move the named root once the complete graph is ready. If an import fails, those
intermediate objects may remain until collection, but the destination root does
not point to a partial graph.

The standard local repository checks disk pressure when a mutation starts and
may run collection first. See [Garbage Collection](../garbage-collection/) for
the policy and explicit cleanup commands.

## Filesystem imports

`FilesystemImport` reads regular files, builds directories from their children,
and publishes the resulting directory under a root. For example:

```console
$ casita --repository ./cache import ./project --root projects/demo
```

The walk opens the source directory and resolves descendants relative to that
open handle. It refuses a linked root and does not follow symlinks or other
reparse points encountered during the walk. A symlink inside the tree is stored
as a link with its target, without reading the target.

On supported local platforms, a repeat import can skip reading a regular file
when its device, inode, size, modification time, and change time match the last
import. This is a speed optimization based on file
metadata, not a fresh content check. A remembered result is used only while its
object still exists in committed repository state. Platforms without a usable
file identity reread every file.

To read and hash every regular file regardless of its metadata, use
`--filesystem-rehash` or `FilesystemImport::new(...).reread(true)` in Rust.
Read [Capture and Restore a Filesystem Tree](../../guides/filesystem/) for the
full workflow.

## Other inputs

| Input | What the importer publishes | Guide |
| --- | --- | --- |
| Git repository | Native Git objects and an immutable ref view under `git/<view>`. | [Git](../../guides/git/) |
| Decompressed tar stream | A canonical filesystem tree, without extracting the archive first. | [Tar](../../guides/tar/) |
| OCI registry image | A single-platform OCI image layout with a bounded config and streamed, digest-verified layers, plus an optional merged filesystem. Requires the `oci` feature. | [OCI](../../guides/oci/) |
| Casitar archive | Verified objects and destination-owned root mappings after the archive's declared closure checks pass. | [Casitar](../../guides/casitar/) |

IPLD and custom formats also publish through ordinary mutation sessions. See
[Add a New Importer](../../guides/adding-an-importer/) for the Rust contract.
