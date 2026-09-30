---
title: Import an OCI Image
description: Stream a registry image into a verified OCI layout and optional merged filesystem.
---

Enable the `oci` Cargo feature to import a container image from an OCI registry.
The importer selects the current operating system and architecture from an image
index by default. It fetches the selected manifest, then downloads its config
and each compressed layer. Casita verifies the registry digests and sizes
before publishing a root that contains a standard single-platform OCI image
layout (`oci-layout`, `index.json`, and `blobs/sha256/`). The layer archives are
stored intact. Add `--oci-rootfs-root NAME` to also create a merged filesystem
that can be checked out or mounted through `casita-fs`.

```console
$ cargo run -p casita --features oci -- --repository ./cache import \
    -i oci registry.example.com/team/app:latest --root images/app \
    --oci-rootfs-root filesystems/app
```

The output includes `root` for the OCI layout and `rootfs` for the merged
filesystem. Pass the printed `rootfs` key to `casita checkout KEY ./rootfs`.

Use `--oci-platform linux/arm64` to select another platform from an index.
For a registry served over plain HTTP, pass `--oci-http` explicitly. The CLI
uses anonymous registry access; Rust callers can pass credentials to
`OciImport::with_auth`, select a platform with `OciImport::with_platform`,
and configure TLS with `OciImport::with_client`.

### Merged filesystem

Rust callers request a filesystem with `OciImport::with_rootfs` and receive
its key in `OciImportReport::rootfs`. The layout and filesystem root names
must be distinct. Both are published in one atomic update after every layer
passes verification, including its uncompressed SHA-256 DiffID from the
image config.

Layers are applied in manifest order as their compressed bytes arrive. A bounded
pipe sends each registry stream to both the original blob store and the layer
decoder, so a merged import does not reopen a stored layer. Repeated layer
descriptors are transferred once per occurrence to apply each changeset.
Uncompressed tar, gzip, and zstd layers are supported, including Docker gzip
layers. Whiteout files remove earlier
entries; opaque directory markers hide earlier children while retaining new
entries in that layer. Files stream into Casita storage without extraction
to a temporary filesystem. Directories and symbolic links are preserved,
and hardlinks share regular file content. The importer never follows a
symbolic link while applying a layer.

The merged tree uses Casita's canonical filesystem model: file contents,
directory names, symbolic link text, and the executable bit. It does not
retain owners, timestamps, other permission bits, xattrs, or hardlink inode
identity. Sparse files and special files such as devices and FIFOs are
rejected in merged mode. The original archives in the layout retain their
metadata. Windows checkout refuses absolute symbolic link targets.

### Resource limits

`OciImportLimits` bounds manifest and config JSON to 16 MiB, each compressed
blob to 256 GiB, all compressed blobs to 1 TiB, and layer count to 1024 by
default. The CLI exposes `--oci-max-blob-bytes` and
`--oci-max-total-blob-bytes`. The registry client buffers a manifest before
Casita applies its JSON size limit. A merged import buffers the config within
that limit to read layer DiffIDs; blob downloads remain bounded streams.

`OciRootfsLimits` additionally bounds decoded layer bytes to 1 TiB per layer
and in total, logical tar entries and merged tree nodes to one million each,
pathnames to 4096 bytes, individual files to 256 GiB, and all staged file
bytes to 1 TiB by default. Overwritten files count toward the staged byte
bound. The CLI exposes `--oci-rootfs-max-bytes` for decoded bytes and
`--oci-rootfs-max-entries` for both entry counts. Directory metadata is kept
in memory within these bounds, while file contents remain streams.

The named roots are published after every selected blob passes verification.
A failed or interrupted import can leave unrooted objects for collection, but
does not replace either existing root. Multi-platform indexes are narrowed to one
selected image; the imported layout does not contain the other platforms.
