//! Orders payload deletions after the metadata commits that allow them.
//!
//! On Apple platforms the state engine syncs its WAL with a plain `fsync`,
//! which leaves committed frames in the drive's volatile cache for the drive
//! to write back in any order. Collection and catalog maintenance delete
//! payloads that a commit made unreachable, so a power loss could keep such a
//! deletion while losing the commit behind it, and the repository would
//! reopen referencing payloads that no longer exist. Commits keep their plain
//! sync; a deletion first makes them durable instead.

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

/// How to make a metadata store's acknowledged commits durable: flush the
/// drive cache holding its database (`F_FULLFSYNC`, which `File::sync_all`
/// issues on Apple platforms) unless the database files are unchanged since
/// the last flush, so a collection pass pays one flush, not one per deletion.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct CommitDurability(Arc<Database>);

#[derive(Debug)]
struct Database {
    files: [PathBuf; 2],
    directory: PathBuf,
    /// Held across a flush, so concurrent deletion batches share one.
    flushed: Mutex<Option<Stamp>>,
    #[cfg(test)]
    flushes: std::sync::atomic::AtomicUsize,
}

/// Length and modification time of the database and its WAL. Any commit or
/// checkpoint writes one of them, so an unchanged stamp means nothing was
/// committed since it was taken.
type Stamp = [Option<(u64, SystemTime)>; 2];

/// A filesystem with whole-second timestamps can hide two writes behind one
/// stamp, so only a stamp with sub-second precision may skip a flush.
fn is_precise(stamp: &Stamp) -> bool {
    stamp.iter().flatten().all(|(_, modified)| {
        modified
            .duration_since(UNIX_EPOCH)
            .is_ok_and(|elapsed| elapsed.subsec_nanos() != 0)
    })
}

impl CommitDurability {
    /// For the SQLite-format database at `database` where a commit's sync can
    /// stop short of stable storage: Apple platforms. Unix test builds use it
    /// everywhere so every CI platform exercises the ordering.
    pub(crate) fn for_database(database: &Path) -> Option<Self> {
        cfg!(any(target_vendor = "apple", all(test, unix))).then(|| Self::new(database))
    }

    fn new(database: &Path) -> Self {
        let mut wal = database.as_os_str().to_owned();
        wal.push("-wal");
        let directory = match database.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        Self(Arc::new(Database {
            files: [database.to_path_buf(), PathBuf::from(wal)],
            directory,
            flushed: Mutex::default(),
            #[cfg(test)]
            flushes: Default::default(),
        }))
    }

    /// Make every commit already written to the database durable.
    async fn ensure(&self) -> io::Result<()> {
        let database = self.0.clone();
        tokio::task::spawn_blocking(move || database.ensure())
            .await
            .map_err(io::Error::other)?
    }

    #[cfg(test)]
    pub(crate) fn flushes(&self) -> usize {
        self.0.flushes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Database {
    fn ensure(&self) -> io::Result<()> {
        let mut flushed = self.flushed.lock().unwrap_or_else(PoisonError::into_inner);
        let mut stamp = [None, None];
        for (slot, path) in stamp.iter_mut().zip(&self.files) {
            *slot = match std::fs::metadata(path) {
                Ok(metadata) => Some((metadata.len(), metadata.modified()?)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error),
            };
        }
        if is_precise(&stamp) && *flushed == Some(stamp) {
            return Ok(());
        }
        // Opening the directory, not the database, leaves the engine's
        // process-scoped file locks alone: closing any descriptor of a locked
        // file would release them.
        File::open(&self.directory)?.sync_all()?;
        *flushed = Some(stamp);
        tracing::debug!("flushed committed state before deleting payloads");
        #[cfg(test)]
        self.flushes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

/// Where a payload store's deletions wait for the commits that allow them.
/// Clones share one set, so pairing a metadata store after the store handed
/// clones to its deleting components still reaches all of them.
#[derive(Clone, Debug, Default)]
pub(crate) struct DeletionBarrier(Arc<Mutex<Vec<CommitDurability>>>);

impl DeletionBarrier {
    pub(crate) fn order_after(&self, commits: CommitDurability) {
        let mut all = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if !all.iter().any(|known| Arc::ptr_eq(&known.0, &commits.0)) {
            all.push(commits);
        }
    }

    /// Make the commits a deletion may depend on durable before it runs.
    pub(crate) async fn before_deletion(&self) -> io::Result<()> {
        let all = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for commits in all {
            commits.ensure().await?;
        }
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    async fn commit(db: &Arc<crate::sqlite::TursoDb>) {
        db.write(|connection| {
            Box::pin(async move {
                connection
                    .execute_batch("CREATE TABLE IF NOT EXISTS t (v); INSERT INTO t VALUES (1);")
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushes_once_after_each_commit_that_precedes_a_deletion() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("casita.sqlite");
        let db = crate::sqlite::TursoDb::open(&path).unwrap();
        let commits = CommitDurability::new(&path);
        let barrier = DeletionBarrier::default();
        barrier.order_after(commits.clone());

        barrier.before_deletion().await.unwrap();
        barrier.before_deletion().await.unwrap();
        assert_eq!(commits.flushes(), 1, "no commit since the last flush");
        commit(&db).await;
        barrier.before_deletion().await.unwrap();
        assert_eq!(commits.flushes(), 2, "a commit since the last flush");
        // A second handle stands in for another process's commit.
        commit(&crate::sqlite::TursoDb::open(&path).unwrap()).await;
        barrier.before_deletion().await.unwrap();
        assert_eq!(commits.flushes(), 3, "another writer's commit");
    }

    #[tokio::test]
    async fn deletions_wait_for_every_paired_metadata_store() {
        let directory = tempfile::tempdir().unwrap();
        let [first, second] = ["first.sqlite", "second.sqlite"].map(|name| {
            let path = directory.path().join(name);
            std::fs::write(&path, b"committed").unwrap();
            CommitDurability::new(&path)
        });
        let barrier = DeletionBarrier::default();
        barrier.order_after(first.clone());
        barrier.order_after(second.clone());
        barrier.order_after(first.clone());
        barrier.before_deletion().await.unwrap();
        assert_eq!((first.flushes(), second.flushes()), (1, 1));
    }

    #[test]
    fn whole_second_timestamps_never_skip_a_flush() {
        let whole = UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let precise = whole + std::time::Duration::from_nanos(1);
        assert!(!is_precise(&[Some((4096, whole)), None]));
        assert!(is_precise(&[Some((4096, precise)), None]));
        assert!(!is_precise(&[Some((4096, precise)), Some((0, whole))]));
    }
}
