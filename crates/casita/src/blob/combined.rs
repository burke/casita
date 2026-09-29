//! A near/far tiered [`BlobStore`].

use async_trait::async_trait;

use super::{BlobReader, BlobStore, BlobStreamReader, BlobWriter, ChunkMeta};
use crate::digest::BlobId;
use crate::error::Error;

/// A [`BlobStore`] layering a writable `near` tier over a read-fallback `far`
/// tier.
///
/// Reads consult `near` first and fall back to `far`; `has` is true if either
/// holds the blob; new blobs are written to `near`. This models a local
/// writable cache in front of a slower or shared upstream you can read from
/// (for example a remote object store). It does **not** copy blobs from `far`
/// into `near` on read.
///
/// ```
/// # #[cfg(feature = "experimental")]
/// # async fn example() -> Result<(), casita::experimental::Error> {
/// use casita::experimental::{BlobStore, CombinedBlobStore, MemoryBlobStore};
///
/// let near = MemoryBlobStore::new();
/// let far = MemoryBlobStore::new();
/// let existing = far.put_slice(b"already upstream").await?;
/// let store = CombinedBlobStore::new(near.clone(), far.clone());
///
/// // Existing payloads remain readable from the far tier.
/// assert_eq!(store.read_to_vec(&existing).await?, Some(b"already upstream".to_vec()));
/// assert!(!near.has(&existing).await?); // reads do not warm the near tier
///
/// // New payloads are written only to the near tier.
/// let added = store.put_slice(b"new payload").await?;
/// assert!(near.has(&added).await?);
/// assert!(!far.has(&added).await?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CombinedBlobStore<N, F> {
    near: N,
    far: F,
}

impl<N, F> CombinedBlobStore<N, F> {
    /// Combine a writable `near` tier with a read-fallback `far` tier.
    pub fn new(near: N, far: F) -> Self {
        Self { near, far }
    }
}

#[async_trait]
impl<N, F> BlobStore for CombinedBlobStore<N, F>
where
    N: BlobStore,
    F: BlobStore,
{
    fn begin_batch(&self) -> super::BlobBatchGuard {
        self.near.begin_batch()
    }

    fn write_scope(&self) -> crate::metadata::BackendWriteScope {
        self.near.write_scope()
    }

    fn begin_pinned_batch(
        &self,
        pin: crate::metadata::DataPinLease,
    ) -> Result<super::BlobBatchGuard, Error> {
        self.near.begin_pinned_batch(pin)
    }

    // Writes land in `near`, so its catalog is the one publication must seal
    // and coordinate with logical state. The read-only `far` tier has none.
    fn publication(&self) -> super::PayloadPublication<'_> {
        self.near.publication()
    }

    fn order_deletions_after(&self, commits: super::CommitDurability) {
        self.near.order_deletions_after(commits);
    }

    async fn has(&self, digest: &BlobId) -> Result<bool, Error> {
        Ok(self.near.has(digest).await? || self.far.has(digest).await?)
    }

    async fn open_read(&self, digest: &BlobId) -> Result<Option<Box<dyn BlobReader>>, Error> {
        if let Some(reader) = self.near.open_read(digest).await? {
            return Ok(Some(reader));
        }
        tracing::debug!(blob = %digest, "blob read falling back to far tier");
        self.far.open_read(digest).await
    }

    async fn open_stream(
        &self,
        digest: &BlobId,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        if let Some(reader) = self.near.open_stream(digest).await? {
            return Ok(Some(reader));
        }
        tracing::debug!(blob = %digest, "blob stream falling back to far tier");
        self.far.open_stream(digest).await
    }

    async fn open_write(&self) -> Box<dyn BlobWriter> {
        self.near.open_write().await
    }

    async fn open_proof(
        &self,
        digest: &BlobId,
        size: u64,
    ) -> Result<Option<Box<dyn BlobStreamReader>>, Error> {
        if let Some(reader) = self.near.open_proof(digest, size).await? {
            return Ok(Some(reader));
        }
        self.far.open_proof(digest, size).await
    }

    async fn overwrite(
        &self,
        digest: &BlobId,
        size: u64,
        offset: u64,
        replacement: &[u8],
    ) -> Result<(BlobId, bytes::Bytes), Error> {
        self.near.overwrite(digest, size, offset, replacement).await
    }

    async fn chunks(&self, digest: &BlobId) -> Result<Option<Vec<ChunkMeta>>, Error> {
        match self.near.chunks(digest).await? {
            Some(chunks) => Ok(Some(chunks)),
            None => {
                tracing::debug!(blob = %digest, "blob chunk lookup falling back to far tier");
                self.far.chunks(digest).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::MemoryBlobStore;
    use crate::test_util::native::{read_blob, small_chunked_store, write_blob};

    #[tokio::test]
    async fn reads_fall_through_to_far() {
        let near = MemoryBlobStore::new();
        let far = MemoryBlobStore::new();
        let digest = write_blob(&far, b"from far").await;

        let combined = CombinedBlobStore::new(near.clone(), far);
        assert!(combined.has(&digest).await.unwrap());
        assert_eq!(
            read_blob(&combined, &digest).await.as_deref(),
            Some(b"from far".as_slice())
        );
        // the read did not populate near.
        assert!(!near.has(&digest).await.unwrap());
    }

    #[tokio::test]
    async fn writes_go_to_near() {
        let near = MemoryBlobStore::new();
        let far = MemoryBlobStore::new();
        let combined = CombinedBlobStore::new(near.clone(), far.clone());

        let digest = write_blob(&combined, b"written").await;
        assert!(near.has(&digest).await.unwrap());
        assert!(!far.has(&digest).await.unwrap());
        assert_eq!(
            read_blob(&combined, &digest).await.as_deref(),
            Some(b"written".as_slice())
        );
    }

    #[tokio::test]
    async fn flush_seals_batched_writes_in_a_packed_near_tier() {
        use object_store::{ObjectStore, memory::InMemory, path::Path};
        use std::sync::Arc;

        let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = Path::from("near");
        let near = crate::blob::ChunkedBlobStore::packed(objects.clone(), base.clone(), 1024)
            .await
            .unwrap();
        let combined = CombinedBlobStore::new(near, MemoryBlobStore::new());

        let batch = combined.begin_batch();
        let digest = write_blob(&combined, b"staged in an open pack").await;
        combined.publication().flush().await.unwrap();

        let reopened = crate::blob::ChunkedBlobStore::packed(objects, base, 1024)
            .await
            .unwrap();
        assert_eq!(
            read_blob(&reopened, &digest).await.as_deref(),
            Some(b"staged in an open pack".as_slice())
        );
        drop(batch);
    }

    #[tokio::test]
    async fn state_commit_prepares_the_near_catalog() {
        use object_store::{memory::InMemory, path::Path};
        use std::sync::Arc;

        let near = crate::blob::ChunkedBlobStore::packed(
            Arc::new(InMemory::new()),
            Path::from("near"),
            1024,
        )
        .await
        .unwrap();
        let combined = CombinedBlobStore::new(near, MemoryBlobStore::new());
        combined.publication().enable_state_catalog();

        let batch = combined.begin_batch();
        write_blob(&combined, b"catalogued").await;
        let prepared = combined.publication().prepare_state_commit().await.unwrap();
        assert!(prepared.catalog().is_some());
        prepared.commit().unwrap();
        drop(batch);
    }

    #[tokio::test]
    async fn missing_everywhere_is_none() {
        let combined = CombinedBlobStore::new(MemoryBlobStore::new(), MemoryBlobStore::new());
        let digest = BlobId::new(blake3::hash(b"absent").into());
        assert!(!combined.has(&digest).await.unwrap());
        assert!(read_blob(&combined, &digest).await.is_none());
        assert_eq!(combined.chunks(&digest).await.unwrap(), None);
    }

    #[tokio::test]
    async fn chunks_fall_through_to_far() {
        let (far, _dir) = small_chunked_store();
        let near = MemoryBlobStore::new();

        // a multi-chunk blob only in far.
        let data: Vec<u8> = (0..20_000).map(|i| (i % 251) as u8).collect();
        let digest = write_blob(&far, &data).await;
        let far_chunks = far.chunks(&digest).await.unwrap().unwrap();
        assert!(far_chunks.len() >= 2, "expected multiple chunks");

        let combined = CombinedBlobStore::new(near, far);
        // chunks() falls through to far's real chunk metadata,
        assert_eq!(combined.chunks(&digest).await.unwrap(), Some(far_chunks));
        // and the blob reads back through the combined store.
        assert_eq!(
            read_blob(&combined, &digest).await.as_deref(),
            Some(data.as_slice())
        );
    }

    #[tokio::test]
    async fn near_shadows_far() {
        let (far, _dir) = small_chunked_store();
        let near = MemoryBlobStore::new();

        // the same multi-chunk blob in both tiers.
        let data: Vec<u8> = (0..20_000).map(|i| (i % 251) as u8).collect();
        let digest = write_blob(&far, &data).await;
        assert_eq!(write_blob(&near, &data).await, digest);
        assert!(far.chunks(&digest).await.unwrap().unwrap().len() >= 2);

        let combined = CombinedBlobStore::new(near, far);
        // near is consulted first: its (empty) chunk list shadows far's.
        assert_eq!(combined.chunks(&digest).await.unwrap(), Some(vec![]));
    }
}
