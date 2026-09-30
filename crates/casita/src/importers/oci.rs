//! Registry image import request.

use async_trait::async_trait;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};

use super::{BackendImporter, Importer};
use crate::RootName;
use crate::blob::BlobStore;
use crate::metadata::MetadataStore;
use crate::oci::{
    ImportOptions, OciImportError, OciImportLimits, OciImportReport, OciRootfsLimits, import_image,
};
use crate::repository::Repository;

/// Stream the current platform of a registry image into a rooted OCI image layout.
///
/// The caller can supply a configured client for private registries and select
/// a platform explicitly. Layers are transferred in manifest order without keeping
/// a complete layer in memory or extracting files to disk.
pub struct OciImport {
    reference: Reference,
    root: RootName,
    client: Client,
    auth: RegistryAuth,
    platform: Option<String>,
    limits: OciImportLimits,
    rootfs: Option<RootName>,
    rootfs_limits: OciRootfsLimits,
}

impl OciImport {
    /// Import an image reference with anonymous registry authentication.
    pub fn new(reference: Reference, root: RootName) -> Self {
        Self {
            reference,
            root,
            client: Client::new(Default::default()),
            auth: RegistryAuth::Anonymous,
            platform: None,
            limits: OciImportLimits::default(),
            rootfs: None,
            rootfs_limits: OciRootfsLimits::default(),
        }
    }

    /// Use a configured registry client, for example for TLS settings.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Use explicit registry credentials.
    pub fn with_auth(mut self, auth: RegistryAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Select an OS/architecture and optional variant from a multi-platform index.
    pub fn with_platform(mut self, platform: impl Into<String>) -> Self {
        self.platform = Some(platform.into());
        self
    }

    /// Set import resource limits.
    pub fn with_limits(mut self, limits: OciImportLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Also publish the merged filesystem under a distinct named root.
    /// Both roots move atomically after all layers and DiffIDs are verified.
    pub fn with_rootfs(mut self, root: RootName) -> Self {
        self.rootfs = Some(root);
        self
    }

    /// Bound decoded layer input and filesystem construction.
    pub fn with_rootfs_limits(mut self, limits: OciRootfsLimits) -> Self {
        self.rootfs_limits = limits;
        self
    }
}

impl<PS, SS> BackendImporter<Repository<PS, SS>> for OciImport
where
    PS: BlobStore,
    SS: MetadataStore,
{
    type Report = OciImportReport;
    type Error = OciImportError;

    async fn import_into(
        self,
        repository: &Repository<PS, SS>,
    ) -> Result<Self::Report, Self::Error> {
        import_image(
            repository,
            self.client,
            self.reference,
            self.auth,
            ImportOptions {
                platform: self.platform,
                root: self.root,
                limits: self.limits,
                rootfs_name: self.rootfs,
                rootfs_limits: self.rootfs_limits,
            },
        )
        .await
    }
}

#[async_trait]
impl Importer for OciImport {
    type Report = OciImportReport;
    type Error = crate::Error;

    async fn import(self, repository: &crate::Repository) -> Result<Self::Report, Self::Error> {
        self.import_into(&repository.inner)
            .await
            .map_err(|error| crate::api::Error::classified(error.category(), error))
    }
}

#[cfg(feature = "experimental")]
repository_importer!(OciImport, [], OciImportReport, OciImportError);
