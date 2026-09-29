---
title: Identifiers
description: Text forms and validation rules for Casita keys, digests, roots, revisions, paths, Git refs, and SSH endpoints.
---

Casita validates identifiers before they reach storage. Lengths below are byte
lengths, not character counts.

## Digests and typed IDs

`Digest` is exactly 32 BLAKE3 bytes. Its canonical text form is:

```text
blake3-<unpadded URL-safe base64 of 32 bytes>
```

The base64 alphabet uses `-` and `_`, never `+` or `/`, and carries no `=`
padding. `BlobId`, `DirectoryId`, and `ChunkId` use the same text form but are
distinct Rust types so callers must select the intended meaning.

`Digest::to_hex()` and `Digest::from_hex()` provide a lowercase 64-character
hex form. Casita uses hex for sharded physical paths; the CLI's short object
form uses the `blake3-...` representation.

## Namespace IDs

A `NamespaceId` matches:

```text
label(.label)*.v<digits>
```

The complete value is ASCII and at most 64 bytes. Each ordinary label starts
with a lowercase ASCII letter; the remaining bytes may be lowercase letters,
digits, or `-`. The last component is `v` followed by one or more digits.

Valid examples include `casita.blob.v1`, `ipld.raw.v1`, and
`git.sha256.commit.v1`.

## Object keys

An `ObjectKey` has the canonical text form:

```text
<namespace>:<unpadded URL-safe base64 native ID>
```

The namespace is at most 64 bytes and the decoded native ID is at most 128
bytes. The native ID is opaque to generic repository code: it is a BLAKE3
digest for several built-in formats, a CID for IPLD, and a native SHA-1 or
SHA-256 OID for Git.

The `ObjectKey` parser does **not** accept `blake3-...`. Selected CLI commands
accept that shorter spelling for filesystem blobs or directories and add the
namespace themselves:

| Command position | Accepted form |
|---|---|
| `object show KEY` | Full object key only |
| `sync --object KEY` | Full object key only |
| `tree list KEY` and `checkout KEY DIR` | Full key or short directory digest |
| `cat KEY` | Full key or short blob digest |
| `root set NAME TARGET` | Full key or an unambiguous short blob/directory digest |

If both a blob and directory record have the same short digest, `root set`
requires the full key.

## Root names

A `RootName` is a durable UTF-8 name split into `/`-separated segments:

- the complete name is 1–1024 bytes;
- each segment is 1–255 bytes;
- leading, trailing, or repeated `/` is invalid;
- `.` and `..` segments are invalid; and
- ASCII C0 control bytes and DEL are invalid.

Other UTF-8 text, including spaces and non-ASCII characters, is permitted.
Prefix operations use segment boundaries: `projects/demo` is under `projects`,
but `projects-old` is not.

CLI filesystem imports without `--root` derive an `auto/...` name from the
canonical source path. Checkout similarly derives `auto/checkout/...` unless
`--no-root` is used. Treat these names as ordinary roots: they retain their
complete closures until removed.

## Repository revisions

`RepositoryRevision` is 32 opaque bytes displayed as:

```text
rev-<unpadded URL-safe base64 of 32 bytes>
```

A successful logical mutation creates a fresh revision. Revisions support
equality checks only: they do not expose ordering and do not identify a state
in another repository.

## Repository generations

`RepositoryGeneration` orders the states of one repository. Every successful
commit advances it atomically with the new revision, and it never decreases,
so of two readers of one repository, the one with the larger generation sees
every commit the other sees. `MetadataReader::generation` and
`RetainedReader::generation` report it; custom metadata backends without
generations return an `Unsupported` error. It is displayed as:

```text
gen-<decimal generation>
```

Generations of different repositories, or of a repository recreated at the
same location, are unrelated.

## Filesystem names

`PathComponent` stores a raw byte name between 1 and 255 bytes. It rejects
`/`, NUL, and the exact names `.` and `..`. Ordering is lexicographic over raw
bytes and defines canonical directory order.

`SymlinkTarget` stores 1–4095 raw bytes. It rejects NUL but permits `/`, `.`,
and `..`. This data-model validation does not imply that every target is safe
to materialize; checkout applies its own platform and traversal protections. A
stored target is link data rather than a path casita resolves: materializing it
creates a link and never follows it, and containment comes from the open
directory handle a checkout writes through.

## Git names

`CanonicalRefName` accepts full names below `refs/`, such as
`refs/heads/main`. The frozen profile follows Git's important safety rules: it
rejects control and space bytes, forbidden punctuation and sequences, leading
dot components, `.lock` suffixes, and names over 1024 bytes.

A CLI Git view name is one non-empty root-name segment. View `origin` is
stored under the ordinary root `git/origin`.

## SSH source endpoints

SSH transfer sources use:

```text
ssh://[user@]host[:port]/absolute/repository/path
```

The repository component must decode to one absolute UTF-8 path of at most 16
KiB and may use URL percent escapes. Query strings and fragments are rejected.
Bracketed IPv6 literals are accepted. User and host parsing is deliberately
strict so no endpoint component becomes shell syntax.

OpenSSH, rather than the URL, supplies identity files, host-key policy, proxy
jumps, agents, and other connection settings.
