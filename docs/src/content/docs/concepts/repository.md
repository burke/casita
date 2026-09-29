---
title: Repository
description: How payloads, object records, roots, and verification fit together.
---

A repository combines payload bytes with revisioned logical state. The
supported `casita::Repository` handle provides local and in-memory versions.
The generic `casita::experimental::Repository<PS, SS>` lets Rust applications
choose backends and a deployment profile.

| Part | Responsibility |
| --- | --- |
| `BlobStore` | Stores plaintext payloads by `BlobId`; it may chunk or compress them internally. |
| `MetadataStore` | Stores immutable object records, named roots, and the repository revision. |
| `FormatRegistry` | Checks each namespace's native identity, canonical payload, and direct links. |
| `RepositoryProfile` | Selects coordination, spill placement, and optional maintenance policy. |

`Repository::local(path).await` uses `path/blobs` for payloads and
`path/casita.sqlite` for logical state, with lock files for coordination across
processes.
`Repository::memory()` is temporary. Generic compositions start without local
coordination or automatic maintenance; their payload and metadata backends
must still preserve the same publication and retention guarantees. See the
[Experimental Rust API](../../reference/experimental-rust-api/#deployment-profiles)
for custom profiles.

## Logical and physical state

An `ObjectKey` identifies one object in its format's namespace. Its
`ObjectRecord` binds that key to a `BlobId`, plaintext length, and ordered
direct links. Several objects may share one payload. A named root points to an
object and retains its complete forward closure.

Chunk boundaries, compression, and Bao proof data are physical storage
details. Changing them does not change an object key, record, or root.

## Publication lifecycle

1. A mutation session protects payloads while they are staged.
2. The registered format verifies each object before its record publishes.
3. Verified records can publish in bounded batches. A named root moves only
   when its entire target closure is complete and valid.
4. Retained reads and transfers hold one stable snapshot while collection may
   reclaim unrelated data.

Failed imports and transfers may leave unrooted objects that collection can
later remove. See [Imports](../imports/), [Roots and Retention](../roots-and-retention/),
and [Garbage Collection](../garbage-collection/) for those lifetimes.

## The locality boundary

A `RepositoryRevision` compares state within one repository. It is not a clock
or a value to order across replicas. A `RepositoryGeneration` orders the states
one repository's readers observe, and is likewise meaningless across
repositories. Roots are also local mappings. Sync can
copy a source root's selected value to a destination, but it does not merge
concurrent name changes.

`MutationSession::publish_if_roots_match` lets a service publish only while
watched roots still have expected values. The service remains responsible for
its own writer policy and any ordering it needs across repositories.
