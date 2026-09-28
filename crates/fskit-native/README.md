# fskit-native

Rust filesystem contracts and native callbacks for macOS FSKit extensions.
The crate has no dependency on Casita. Its backend contract uses only standard
library types, while the macOS adapter uses `objc2`, `objc2-foundation`,
`objc2-fs-kit`, and `block2`.

## Backend contract

Implement `Filesystem` to provide metadata, lookup, directory snapshots, reads,
symlinks, and shutdown. Implement `Control` if the backend also handles
application-specific operations. `BackendSession` admits concurrent callbacks
and coordinates retryable shutdown. The consumer remains responsible for OS
unmounting.

Names and symlink targets are byte sequences. Reads fill caller-provided
buffers, or can return borrowed or owned initialized bytes to the native
adapter. Directory snapshots retain optional attributes across paginated
enumeration. See the [contract design and limits](https://github.com/cachix/casita/blob/main/crates/fskit-native/CONTRACT.md)
for the full backend requirements.

## Native extension

The extension executable calls `native::register` from its constructor with
an `Extension` that specifies a display name, filesystem type, accepted
resource kinds, and backend factory. Set `EXExtensionPrincipalClass` to
`FSKitNativeFileSystem` in its Info.plist and link Apple's extension entry
point. The adapter supports path resources and block-device-backed fixtures.
No Swift or separate IPC bridge is required.

The adapter serves immutable files, directories, and symlinks. It provides an
empty xattr namespace or uses FSKit emulation, and it can delegate creation to
the backend for application control operations. Ordinary writes and other
mutations return `EROFS`. Volume statistics use fixed synthetic values.

The consumer owns its app identity, signing, packaging, and mount lifecycle.

## Registration and mounting

The optional `setup` feature provides rootless registration and activation on
macOS:

```rust,ignore
setup::register(app, "Example.appex", "org.example.filesystem")?;
```

Registration verifies the signature and module identity, serializes settings
changes across modules, and replaces an installed bundle only when no FSKit
volumes are mounted. A registration change restarts the agent, even when the
module was already enabled. An interrupted activation can be retried. Use a
new app path for upgrades.

For a normal mount, call `setup::installed` to find and verify the selected
bundle without changing registration or restarting the agent. Keep the
returned `Installation` alive until unmounting. Its shared lock allows other
mounts while preventing setup from replacing their installation. `app()`
identifies the installed bundle, and `property()` reads signed extension
metadata. Pending activation requires an explicit setup call.

Automatic activation requires access to macOS's protected FSKit settings. If
macOS denies that access, setup can still complete registration and any
required agent restart. Attempt the mount to let macOS check user approval;
if it fails, direct the user to **System Settings → General → Login Items &
Extensions → By Category → File System Extensions**. Other settings errors
stop setup before registration changes.

## Development and license

From the Casita repository root, run the crate tests with:

```sh
cargo test -p fskit-native --all-features
```

Casita's memory fixture and repository backend also exercise the contract:

```sh
cargo test --locked --manifest-path crates/casita-fskit/Cargo.toml --features repository --lib
```

The native adapter derives from the objc2 FSKit example. Its MIT attribution
is preserved in `LICENSE-MIT.txt`.
