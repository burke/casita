# Releasing Casita

This document defines the intended release contract and the checklist for a
Casita maintainer. The project is still pre-release. The first release needs
better performance and benchmark coverage, validation through practical use,
an automated tagged-release workflow, and a successful dry run.

## Distribution contract

The initial supported distribution is:

- an annotated Git tag named `v<version>`;
- a GitHub Release attached to that exact tag;
- a deterministic source archive produced from the tag;
- prebuilt CLI archives for the tested release targets; and
- `SHA256SUMS` covering every attached artifact.

Crates.io publication of the `casita` and `casita-fs` packages is not part of
the initial contract. Optional WAL3 and Chroma dependencies remain pinned to
Git revisions and cannot currently be represented by a publishable crates.io
package. Do not treat `cargo publish` or `cargo package` for those packages as
a release gate until that policy changes explicitly. The independent
`fskit-native` crate can be published separately; its package verification
does not establish that Casita's source or binary release is ready.

The initial binary feature profile is `cli,git,ssh`. `cli` enables the
`experimental` API internally; release binaries exclude the optional S3,
Git fetch, and Git HTTP profiles.

| Capability | Release status |
|---|---|
| Portable library (`--no-default-features`) | Supported from the source tag |
| `native` repository library | Supported from the source tag |
| `cli` | Supported in source and binary artifacts |
| `git` native import | Supported in source and binary artifacts |
| `ssh` transfer | Supported in source and binary artifacts |
| `ipc` | Bundled where required by `cli`; no separate wire-compatibility promise |
| `s3` | Experimental; source only |
| `git-fetch` | Experimental; source only |
| `git-http` | Experimental; source only |

The initial binary targets are the targets exercised by release CI:

- `x86_64-unknown-linux-gnu`;
- `aarch64-apple-darwin`; and
- `x86_64-pc-windows-msvc`.

Adding a target requires a CI runner that executes the supported feature
profile and validates the produced archive. Removing a target requires release
notes and a support-matrix update.

## Independent `fskit-native` crate

`fskit-native` has no Casita dependency and can be released to crates.io on its
own version line. Its release does not use the `v<version>` tags reserved for
Casita. Before publishing, confirm the crate version and README. Check that the
Rust 1.94.1, Linux, and macOS CI jobs pass for the exact candidate commit, and
inspect the package contents with `cargo package -p fskit-native --list`. Run
`cargo package -p fskit-native` and `cargo publish -p fskit-native --dry-run`
from a clean checkout. Publish only the verified candidate, then record it with
an annotated `fskit-native-v<version>` tag and release notes. Do not move a
published tag or reuse a published crate version.

## Compatibility and lockfile policy

Package versions follow Semantic Versioning. Before 1.0, a minor release may
change public Rust or CLI interfaces; patch releases must remain compatible
within the documented release surface.

Frozen identities, canonical encodings, and verification semantics are
versioned independently and are not weakened by package-version policy. Physical
layouts and repository schemas follow their separately documented compatibility
and migration rules.

`Cargo.lock` is committed and authoritative for CLI binaries, CI, benchmarks,
and release artifacts. Library consumers selecting Casita from a source tag may
resolve dependencies according to `Cargo.toml`; they must commit their own lock
file when building an application reproducibly.

## Release prerequisites

Before creating a release candidate:

1. Select a version and verify that `Cargo.toml`, `Cargo.lock`, documentation,
   and the CLI report it consistently.
2. Move the relevant entries from **Unreleased** in `CHANGELOG.md` into a
   dated `## [<version>] - YYYY-MM-DD` section and update its comparison links.
3. Confirm that `SECURITY.md` names the release line that will receive fixes.
4. Confirm that the declared `rust-version` is exercised by CI and that the
   pinned development toolchain remains green.
5. Confirm that every supported platform and feature profile passes from a
   clean checkout with `--locked` dependency resolution.
6. Confirm that documentation, hostile-input checks, dependency policy, API
   compatibility checks, coverage policy, and release-facing benchmarks are
   green for the candidate commit.
7. Confirm that no generated artifact or source archive contains credentials,
   local paths, benchmark secrets, or an uncommitted worktree.
8. Review release-facing benchmark coverage and results on representative
   workloads. Improve unacceptable bottlenecks, record reproducible candidate
   results, and keep new benchmark cases in the permanent corpus.
9. Exercise the supported import, checkout, sync, and collection workflows on
   real projects beyond the release smoke tests. Record and fix issues that
   would block practical use of 0.1.

At minimum, the candidate must pass:

```console
$ devenv shell cargo fmt --all -- --check
$ devenv shell cargo test --all-features --locked
$ devenv shell cargo test --locked
$ devenv shell cargo test --no-default-features --locked
$ RUSTDOCFLAGS="-D warnings" devenv shell cargo doc --all-features --no-deps --locked
```

The release-policy workflow's dependency job runs `cargo-deny` with its pinned
tooling and policy. The workflow is authoritative when it is stricter than this
local list. A blocked or skipped required job is not a successful release gate.

## Dry run

The tagged-release workflow must support a non-publishing dry run from an exact
commit. The dry run must:

1. build the supported source and binary artifacts in clean workers;
2. record the commit, Rust and Cargo versions, target, features, and lockfile
   digest in build metadata;
3. generate `SHA256SUMS`;
4. unpack each archive and run `casita --version`;
5. perform a minimal import, root inspection, checkout, and local sync smoke
   test with each binary; and
6. upload temporary artifacts for maintainer inspection without creating a Git
   tag or public release.

Do not create the first public tag until this dry run has completed
successfully on every supported target.

## Tag and publish

Once the exact candidate commit has passed every gate:

1. Verify that `main` is clean, pushed, and contains the candidate commit.
2. Create an annotated tag:

   ```console
   $ git tag -a v<version> -m "Casita v<version>"
   ```

3. Push the tag without moving or force-updating any existing tag.
4. Let the tagged-release workflow build artifacts from the tag rather than
   from an ambient checkout.
5. Verify artifact checksums, build metadata, archive contents, and smoke-test
   results before publishing the GitHub Release.
6. Publish release notes derived from `CHANGELOG.md`, including supported
   platforms, features, known limitations, and any migration or repair steps.

Release automation must use least-privilege GitHub permissions and must not
expose repository or signing credentials to untrusted pull-request code.

## Failed releases

Never rewrite a published tag or replace files silently. If a release is
incorrect, mark it as affected, preserve enough information for users to
identify it, and publish a new patch or pre-release version. Security-sensitive
failures follow `SECURITY.md` and may require coordinated disclosure before the
replacement is public.
