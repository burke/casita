---
title: Import an OCI Image
description: Import a container image, keep its original layers, and check out its merged filesystem.
---

Import a container image from a registry into Casita. You can keep the image's
original layers and also combine them into a filesystem to check out locally.

## 1. Enable OCI support

From a Casita source checkout, install the CLI with the `oci` feature:

```console
$ cargo install --path crates/casita --features oci
```

See [Cargo Features](../../reference/cargo-features/) for other build options.

## 2. Import an image

This example imports Alpine and keeps both the image and its filesystem in
the repository at `./cache`:

```console
$ casita --repository ./cache import \
    -i oci docker.io/library/alpine:latest \
    --root images/alpine \
    --oci-rootfs-root filesystems/alpine
```

The two names refer to different things:

| Option | What Casita keeps under that name |
| --- | --- |
| `--root images/alpine` | The image layout: its manifest, config, and original layer archives. |
| `--oci-rootfs-root filesystems/alpine` | The merged filesystem: the files and directories left after applying every layer. |

The names must differ. To keep only the image layout, omit
`--oci-rootfs-root`.

Casita verifies the downloaded content before publishing either name. If the
import fails, any previous image or filesystem under those names remains
available. An interrupted import may leave unused objects that garbage
collection can remove.

## 3. Check out the filesystem

The import prints a `root` key for the image layout and a `rootfs` key for the
merged filesystem. Copy the directory key printed after `rootfs`, then use
it in place of `ROOTFS_KEY` below:

```console
$ casita --repository ./cache checkout ROOTFS_KEY ./alpine-rootfs
```

Use the same repository path as the import. The destination must be absent
or empty. The merged filesystem can also be mounted through `casita-fs`.

To check out the original image layout instead, use the key printed after
`root`. That directory contains `oci-layout`, `index.json`, and
`blobs/sha256/`.

## Choose a platform

For an image that offers several platforms, Casita selects your current
operating system and architecture. Add `--oci-platform` to select another:

```console
$ casita --repository ./cache import \
    -i oci docker.io/library/alpine:latest \
    --root images/alpine-arm64 \
    --oci-rootfs-root filesystems/alpine-arm64 \
    --oci-platform linux/arm64
```

Each import keeps one selected platform. Other platforms from the image index
are not included in its layout.

The CLI connects over HTTPS and uses anonymous registry access. Use
`--oci-http` for a registry served over plain HTTP. Private registry
credentials can be supplied through the Rust API below.

## How layers become a filesystem

Casita applies layers in image order. Later layers can replace files or remove
them using deletion markers called *whiteouts*. An opaque directory marker
removes children inherited from earlier layers while keeping new children
from its own layer.

The importer supports uncompressed tar, gzip, and zstd layers, including
Docker gzip layers. It preserves file contents, directory names, symbolic
link targets, and the executable bit. Hardlinks share the same stored file
content, and symbolic links are stored without following their targets.

Owners, timestamps, other permission bits, extended attributes, and hardlink
inode identity are not preserved in the merged filesystem. The original layer
archives retain this metadata. Sparse files and special files such as devices
and FIFOs are rejected when building a merged filesystem. Windows checkout
refuses absolute symbolic link targets.

Each layer streams into storage as it downloads. When a merged filesystem is
requested, the same transfer also feeds the decoder and stores the files
inside it. No complete layer is buffered in memory or reread from storage.
A repeated layer descriptor is downloaded once for each occurrence.

Casita checks each downloaded blob's digest and size. It also checks the
uncompressed layer digest, called a *DiffID*, against the image config.
Both named roots are updated together only after all checks pass.

## Resource limits

Large file contents stream through the importer, while directory metadata
stays in memory. The following default limits bound an import:

| Resource | Default limit |
| --- | --- |
| Manifest or config JSON | 16 MiB each |
| Layers | 1024 |
| One downloaded config or layer blob | 256 GiB |
| All downloaded config and layer blobs | 1 TiB |
| Decoded tar bytes | 1 TiB per layer and in total |
| Layer entries and merged filesystem nodes | 1,000,000 each |
| Pathname or hardlink target | 4096 bytes |
| One regular file | 256 GiB |
| File content stored across all layers | 1 TiB, including overwritten files |

Use `--oci-max-blob-bytes` and `--oci-max-total-blob-bytes` to change the
download limits. For a merged filesystem, `--oci-rootfs-max-bytes` sets the
decoded tar limit per layer and in total, and `--oci-rootfs-max-entries` sets
both entry limits. Byte limits are supplied as integers.

The registry client buffers manifests before Casita checks their JSON size.
Building a merged filesystem also buffers the config within its limit so
Casita can read the expected layer digests.

## Use from Rust

Enable the `oci` feature and pass an `OciImport` request to
`Repository::import`. `OciImport::new` takes an image reference and the name
for its layout. Add `with_rootfs` with a different name to request a merged
filesystem; its key is returned in `OciImportReport::rootfs`.

Use `with_auth` to supply registry credentials, `with_platform` to select a
platform, and `with_client` to configure the registry client, including TLS.
`with_limits` accepts `OciImportLimits`, and `with_rootfs_limits` accepts
`OciRootfsLimits` for the remaining resource limits.

See the [CLI Reference](../../reference/cli/#import--i-oci) for the full
command syntax and the [Library guide](../../library/) for repository setup.
