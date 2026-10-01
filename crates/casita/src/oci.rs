//! Streaming registry image import into an OCI image layout and optional merged filesystem.

use std::collections::BTreeMap;
use std::io;

use futures::TryStreamExt;
use oci_client::client::current_platform_resolver;
use oci_client::manifest::{
    IMAGE_MANIFEST_LIST_MEDIA_TYPE, IMAGE_MANIFEST_MEDIA_TYPE, ImageIndexEntry,
    OCI_IMAGE_INDEX_MEDIA_TYPE, OCI_IMAGE_MEDIA_TYPE, OciDescriptor, OciManifest,
};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::StreamReader;

use crate::blob::BlobStore;
use crate::directory::Directory;
use crate::metadata::{MetadataStore, RootChange};
use crate::node::Node;
use crate::object::{ObjectKey, RootName};
use crate::path::PathComponent;
use crate::repository::{Repository, RepositoryError, RepositoryErrorCategory, StagedObject};

mod rootfs;
pub use rootfs::OciRootfsLimits;

/// Bounds applied to a registry image import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OciImportLimits {
    /// Maximum raw manifest or config bytes. The registry client buffers manifests before this check.
    pub max_json_bytes: usize,
    /// Maximum number of layer descriptors.
    pub max_layers: usize,
    /// Maximum bytes in one config or compressed layer blob.
    pub max_blob_bytes: u64,
    /// Maximum aggregate config and compressed layer bytes.
    pub max_total_blob_bytes: u64,
}

impl Default for OciImportLimits {
    fn default() -> Self {
        Self {
            max_json_bytes: 16 << 20,
            max_layers: 1024,
            max_blob_bytes: 1 << 38,
            max_total_blob_bytes: 1 << 40,
        }
    }
}

/// Identity of the published image layout and optional merged filesystem.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OciImportReport {
    /// Casita directory key for the complete OCI image layout.
    pub root: ObjectKey,
    /// Merged canonical filesystem key, when requested.
    pub rootfs: Option<ObjectKey>,
    /// SHA-256 digest of the selected image manifest.
    pub manifest_digest: String,
    /// SHA-256 digest of the source index, if the reference selected a multi-platform index.
    pub source_index_digest: Option<String>,
    /// Number of compressed layers in manifest order.
    pub layers: usize,
    /// Total config and compressed layer bytes.
    pub blob_bytes: u64,
}

/// Registry, image validation, or repository failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OciImportError {
    /// Registry access failed.
    #[error("OCI registry request failed: {0}")]
    Registry(#[from] oci_client::errors::OciDistributionError),
    /// A manifest, descriptor, or downloaded blob was invalid.
    #[error("invalid OCI image: {0}")]
    Invalid(String),
    /// A layer compression or filesystem entry cannot be represented.
    #[error("unsupported OCI filesystem input: {0}")]
    Unsupported(String),
    /// A configured input bound was exceeded.
    #[error("OCI import {field} {actual} exceeds limit {limit}")]
    LimitExceeded {
        /// Bound that failed.
        field: &'static str,
        /// Observed value.
        actual: u64,
        /// Configured maximum.
        limit: u64,
    },
    /// Reading a blob stream failed.
    #[error("OCI blob stream failed: {0}")]
    Io(#[from] io::Error),
    /// Encoding the generated index failed.
    #[error("OCI index encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    /// A Casita directory could not be assembled.
    #[error("OCI layout directory failed: {0}")]
    Directory(#[from] crate::DirectoryError),
    /// Staging or publication failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

impl OciImportError {
    /// Stable application error classification.
    pub fn category(&self) -> RepositoryErrorCategory {
        match self {
            Self::Repository(error) => error.category(),
            Self::Unsupported(_) => RepositoryErrorCategory::Unsupported,
            Self::Invalid(_) | Self::LimitExceeded { .. } | Self::Directory(_) => {
                RepositoryErrorCategory::InvalidInput
            }
            Self::Io(error)
                if matches!(
                    error.kind(),
                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                ) =>
            {
                RepositoryErrorCategory::InvalidInput
            }
            Self::Json(_) => RepositoryErrorCategory::InvalidInput,
            Self::Registry(_) | Self::Io(_) => RepositoryErrorCategory::Backend,
        }
    }
}

fn check_limit(field: &'static str, actual: u64, limit: u64) -> Result<(), OciImportError> {
    if actual > limit {
        Err(OciImportError::LimitExceeded {
            field,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn digest_name(digest: &str) -> Result<&str, OciImportError> {
    let Some(hex) = digest.strip_prefix("sha256:") else {
        return Err(OciImportError::Invalid(format!(
            "unsupported digest {digest}; expected sha256"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(OciImportError::Invalid(format!(
            "malformed digest {digest}"
        )));
    }
    Ok(hex)
}

fn verify_json(bytes: &[u8], digest: &str, limit: usize) -> Result<(), OciImportError> {
    check_limit("JSON bytes", bytes.len() as u64, limit as u64)?;
    digest_name(digest)?;
    if sha256_hex(bytes) != digest {
        return Err(OciImportError::Invalid(format!(
            "digest mismatch for {digest}"
        )));
    }
    Ok(())
}

fn blob_node(object: &StagedObject<'_>) -> Node {
    Node::File {
        digest: object.record().payload(),
        size: object.record().payload_size(),
        executable: false,
    }
}

fn component(name: &str) -> PathComponent {
    PathComponent::try_from(name).expect("fixed OCI layout component or validated sha256 hex")
}

fn select_platform(
    entries: &[ImageIndexEntry],
    wanted: Option<&str>,
) -> Result<String, OciImportError> {
    let Some(wanted) = wanted else {
        return current_platform_resolver(entries)
            .ok_or_else(|| OciImportError::Invalid("no image for the current platform".into()));
    };
    let parts: Vec<_> = wanted.split('/').collect();
    if !(2..=3).contains(&parts.len()) || parts.iter().any(|part| part.is_empty()) {
        return Err(OciImportError::Invalid(
            "platform must be OS/ARCH[/VARIANT]".into(),
        ));
    }
    entries
        .iter()
        .find(|entry| {
            entry.platform.as_ref().is_some_and(|platform| {
                platform.os.to_string() == parts[0]
                    && platform.architecture.to_string() == parts[1]
                    && (parts.len() == 2 || platform.variant.as_deref() == Some(parts[2]))
            })
        })
        .map(|entry| entry.digest.clone())
        .ok_or_else(|| OciImportError::Invalid(format!("no image for platform {wanted}")))
}

fn account_blob(
    descriptor: &OciDescriptor,
    limits: OciImportLimits,
    total: &mut u64,
) -> Result<u64, OciImportError> {
    digest_name(&descriptor.digest)?;
    let declared = u64::try_from(descriptor.size)
        .map_err(|_| OciImportError::Invalid("negative blob size".into()))?;
    check_limit("blob bytes", declared, limits.max_blob_bytes)?;
    *total = total
        .checked_add(declared)
        .ok_or_else(|| OciImportError::Invalid("total blob size overflow".into()))?;
    check_limit("total blob bytes", *total, limits.max_total_blob_bytes)?;
    Ok(declared)
}

async fn pull_config<'a, PS, SS>(
    mutation: &'a crate::repository::MutationSession<'_, PS, SS>,
    client: &Client,
    reference: &Reference,
    descriptor: &OciDescriptor,
    limits: OciImportLimits,
    total: &mut u64,
) -> Result<(StagedObject<'a>, Vec<u8>), OciImportError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let declared = account_blob(descriptor, limits, total)?;
    check_limit("config bytes", declared, limits.max_json_bytes as u64)?;
    // The config is small and needed before decoding any layer. Buffer it once,
    // with one spare byte to distinguish an oversized response from EOF.
    let stream = client.pull_blob_stream(reference, descriptor).await?;
    let reader = StreamReader::new(stream.map_err(io::Error::other));
    let mut bounded = reader.take((limits.max_json_bytes as u64).saturating_add(1));
    let mut bytes = Vec::new();
    bounded.read_to_end(&mut bytes).await?;
    check_limit(
        "config bytes",
        bytes.len() as u64,
        limits.max_json_bytes as u64,
    )?;
    if bytes.len() as u64 != declared {
        return Err(OciImportError::Invalid(format!(
            "config {} has {} bytes, expected {declared}",
            descriptor.digest,
            bytes.len()
        )));
    }
    let object = mutation.stage_blob(&bytes).await?;
    Ok((object, bytes))
}

async fn pull_blob<'a, PS, SS>(
    mutation: &'a crate::repository::MutationSession<'_, PS, SS>,
    client: &Client,
    reference: &Reference,
    descriptor: &OciDescriptor,
    limits: OciImportLimits,
    total: &mut u64,
) -> Result<StagedObject<'a>, OciImportError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let declared = account_blob(descriptor, limits, total)?;
    // The client verifies the descriptor digest when the stream reaches EOF.
    let stream = client.pull_blob_stream(reference, descriptor).await?;
    let reader = StreamReader::new(stream.map_err(io::Error::other));
    let mut bounded = reader.take(limits.max_blob_bytes.saturating_add(1));
    let object = mutation.stage_blob_reader(&mut bounded).await?;
    let observed = object.record().payload_size();
    check_limit("blob bytes", observed, limits.max_blob_bytes)?;
    if observed != declared {
        return Err(OciImportError::Invalid(format!(
            "blob {} has {} bytes, expected {}",
            descriptor.digest, observed, declared
        )));
    }
    Ok(object)
}

/// Fan one bounded registry stream out to the original blob and the decoder.
/// The decoded consumer reads to EOF, so both the compressed digest and DiffID
/// finish verification before this function returns.
async fn pull_and_apply_layer<'a, PS, SS>(
    mutation: &'a crate::repository::MutationSession<'_, PS, SS>,
    client: &Client,
    reference: &Reference,
    descriptor: &OciDescriptor,
    limits: OciImportLimits,
    total: &mut u64,
    rootfs: &mut rootfs::RootfsBuilder,
) -> Result<StagedObject<'a>, OciImportError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let declared = account_blob(descriptor, limits, total)?;
    let stream = client.pull_blob_stream(reference, descriptor).await?;
    let (mut archive_writer, mut archive_reader) = tokio::io::duplex(64 * 1024);
    let tee: std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<bytes::Bytes, io::Error>> + Send>,
    > = Box::pin(async_stream::try_stream! {
        let mut stream = stream;
        let mut received = 0u64;
        while let Some(chunk) = stream.try_next().await.map_err(io::Error::other)? {
            received = received.checked_add(chunk.len() as u64)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "compressed OCI layer size overflow"))?;
            if received > limits.max_blob_bytes {
                Err(io::Error::new(io::ErrorKind::InvalidData, "compressed OCI layer exceeds byte limit"))?;
            }
            archive_writer.write_all(&chunk).await?;
            yield chunk;
        }
        archive_writer.shutdown().await?;
    });
    let decoder_input = StreamReader::new(tee);
    let stage = async {
        mutation
            .stage_blob_reader(&mut archive_reader)
            .await
            .map_err(OciImportError::from)
    };
    let apply = rootfs.apply(mutation, decoder_input, &descriptor.media_type);
    let (object, ()) = tokio::try_join!(stage, apply)?;
    let observed = object.record().payload_size();
    check_limit("blob bytes", observed, limits.max_blob_bytes)?;
    if observed != declared {
        return Err(OciImportError::Invalid(format!(
            "blob {} has {} bytes, expected {declared}",
            descriptor.digest, observed
        )));
    }
    Ok(object)
}

/// Import a registry reference as a complete single-platform OCI image layout.
pub(crate) struct ImportOptions {
    pub platform: Option<String>,
    pub root: RootName,
    pub limits: OciImportLimits,
    pub rootfs_name: Option<RootName>,
    pub rootfs_limits: OciRootfsLimits,
}

pub(crate) async fn import_image<PS, SS>(
    repository: &Repository<PS, SS>,
    client: Client,
    reference: Reference,
    auth: RegistryAuth,
    options: ImportOptions,
) -> Result<OciImportReport, OciImportError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let ImportOptions {
        platform,
        root,
        limits,
        rootfs_name,
        rootfs_limits,
    } = options;
    let batch = repository.limits().max_batch_objects;
    if batch == 0 {
        return Err(OciImportError::Invalid(
            "repository mutation batch limit must be positive".into(),
        ));
    }
    if rootfs_name.as_ref() == Some(&root) {
        return Err(OciImportError::Invalid(
            "layout and filesystem roots must have distinct names".into(),
        ));
    }
    if rootfs_name.is_some() {
        rootfs_limits.validate()?;
    }
    check_limit(
        "root changes",
        1 + u64::from(rootfs_name.is_some()),
        repository.limits().max_root_changes as u64,
    )?;
    if limits.max_json_bytes == 0 || limits.max_blob_bytes == 0 || limits.max_total_blob_bytes == 0
    {
        return Err(OciImportError::Invalid(
            "OCI import limits must be positive".into(),
        ));
    }
    let media_types = [
        OCI_IMAGE_MEDIA_TYPE,
        OCI_IMAGE_INDEX_MEDIA_TYPE,
        IMAGE_MANIFEST_MEDIA_TYPE,
        IMAGE_MANIFEST_LIST_MEDIA_TYPE,
    ];
    let (top_bytes, top_digest) = client
        .pull_manifest_raw(&reference, &auth, &media_types)
        .await?;
    verify_json(&top_bytes, &top_digest, limits.max_json_bytes)?;
    let top: OciManifest = serde_json::from_slice(&top_bytes)?;
    let (manifest_bytes, manifest_digest, source_index_digest, image) = match top {
        OciManifest::Image(image) => (top_bytes, top_digest, None, image),
        OciManifest::ImageIndex(index) => {
            if index.schema_version != 2 {
                return Err(OciImportError::Invalid(
                    "unsupported image index schema".into(),
                ));
            }
            let selected = select_platform(&index.manifests, platform.as_deref())?;
            let descriptor = index
                .manifests
                .iter()
                .find(|item| item.digest == selected)
                .ok_or_else(|| {
                    OciImportError::Invalid("selected manifest is absent from index".into())
                })?;
            let selected_reference = reference.clone_with_digest(selected.clone());
            let (bytes, returned_digest) = client
                .pull_manifest_raw(&selected_reference, &auth, &media_types)
                .await?;
            verify_json(&bytes, &selected, limits.max_json_bytes)?;
            if returned_digest != selected || bytes.len() as i64 != descriptor.size {
                return Err(OciImportError::Invalid(
                    "selected manifest descriptor mismatch".into(),
                ));
            }
            let OciManifest::Image(image) = serde_json::from_slice(&bytes)? else {
                return Err(OciImportError::Invalid(
                    "selected manifest is another index".into(),
                ));
            };
            (bytes, selected, Some(top_digest), image)
        }
    };
    if image.schema_version != 2 {
        return Err(OciImportError::Invalid(
            "unsupported image manifest schema".into(),
        ));
    }
    check_limit(
        "layers",
        image.layers.len() as u64,
        limits.max_layers as u64,
    )?;
    let layout = b"{\"imageLayoutVersion\":\"1.0.0\"}\n";
    let index = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": OCI_IMAGE_INDEX_MEDIA_TYPE,
        "manifests": [{
            "mediaType": image.media_type.as_deref().unwrap_or(OCI_IMAGE_MEDIA_TYPE),
            "digest": manifest_digest,
            "size": manifest_bytes.len(),
        }],
    });
    let index_bytes = serde_json::to_vec(&index)?;

    let mutation = repository.mutation_session().await?;
    let mut staged = Vec::new();
    let mut blobs = BTreeMap::new();
    let manifest_object = mutation.stage_blob(&manifest_bytes).await?;
    blobs.insert(
        digest_name(&manifest_digest)?.to_owned(),
        blob_node(&manifest_object),
    );
    staged.push(manifest_object);

    let mut total = 0;
    let (config_object, config_bytes) = if rootfs_name.is_some() {
        let (object, bytes) = pull_config(
            &mutation,
            &client,
            &reference,
            &image.config,
            limits,
            &mut total,
        )
        .await?;
        (object, Some(bytes))
    } else {
        let config_limits = OciImportLimits {
            max_blob_bytes: limits.max_blob_bytes.min(limits.max_json_bytes as u64),
            ..limits
        };
        let object = pull_blob(
            &mutation,
            &client,
            &reference,
            &image.config,
            config_limits,
            &mut total,
        )
        .await?;
        (object, None)
    };
    blobs.insert(
        digest_name(&image.config.digest)?.to_owned(),
        blob_node(&config_object),
    );
    staged.push(config_object);
    let mut rootfs = if let Some(config_bytes) = config_bytes.as_deref() {
        Some(rootfs::RootfsBuilder::new(
            config_bytes,
            image.layers.len(),
            rootfs_limits,
        )?)
    } else {
        None
    };
    drop(config_bytes);
    for descriptor in &image.layers {
        let name = digest_name(&descriptor.digest)?;
        let mut present = false;
        if let Some(Node::File { size, .. }) = blobs.get(name) {
            if i64::try_from(*size).ok() != Some(descriptor.size) {
                return Err(OciImportError::Invalid(format!(
                    "conflicting sizes for blob {}",
                    descriptor.digest
                )));
            }
            present = true;
        }
        if present && rootfs.is_none() {
            continue;
        }
        let object = if let Some(builder) = rootfs.as_mut() {
            pull_and_apply_layer(
                &mutation, &client, &reference, descriptor, limits, &mut total, builder,
            )
            .await?
        } else {
            pull_blob(
                &mutation, &client, &reference, descriptor, limits, &mut total,
            )
            .await?
        };
        if !present {
            blobs.insert(name.to_owned(), blob_node(&object));
            staged.push(object);
        }
        if staged.len() >= batch {
            while !staged.is_empty() {
                let count = staged.len().min(batch);
                mutation
                    .publish_unrooted(staged.drain(..count).collect())
                    .await?;
            }
        }
    }
    // Every intermediate object remains protected by this mutation session.
    while !staged.is_empty() {
        let count = staged.len().min(batch);
        mutation
            .publish_unrooted(staged.drain(..count).collect())
            .await?;
    }
    let rootfs_key = if let Some(builder) = rootfs {
        Some(builder.finish(&mutation).await?)
    } else {
        None
    };
    let sha_dir = Directory::try_from_iter(
        blobs
            .into_iter()
            .map(|(digest, node)| (component(&digest), node)),
    )?;
    let sha_node = Node::Directory {
        digest: sha_dir.digest(),
        size: sha_dir.size(),
    };
    staged.push(mutation.stage_directory(&sha_dir).await?);
    let blobs_dir = Directory::try_from_iter([(component("sha256"), sha_node)])?;
    let blobs_node = Node::Directory {
        digest: blobs_dir.digest(),
        size: blobs_dir.size(),
    };
    staged.push(mutation.stage_directory(&blobs_dir).await?);
    let layout_object = mutation.stage_blob(layout).await?;
    let index_object = mutation.stage_blob(&index_bytes).await?;
    let root_dir = Directory::try_from_iter([
        (component("blobs"), blobs_node),
        (component("index.json"), blob_node(&index_object)),
        (component("oci-layout"), blob_node(&layout_object)),
    ])?;
    staged.push(layout_object);
    staged.push(index_object);
    let root_key = ObjectKey::directory(root_dir.digest());
    staged.push(mutation.stage_directory(&root_dir).await?);
    while !staged.is_empty() {
        let count = staged.len().min(batch);
        mutation
            .publish_unrooted(staged.drain(..count).collect())
            .await?;
    }
    let mut roots = vec![RootChange::Set {
        name: root,
        target: root_key.clone(),
    }];
    if let (Some(name), Some(target)) = (rootfs_name, rootfs_key.as_ref()) {
        roots.push(RootChange::Set {
            name,
            target: target.clone(),
        });
    }
    mutation.publish(Vec::new(), roots).await?;
    Ok(OciImportReport {
        root: root_key,
        rootfs: rootfs_key,
        manifest_digest,
        source_index_digest,
        layers: image.layers.len(),
        blob_bytes: total,
    })
}
