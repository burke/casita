//! Opt-in state contention measurements with snapshot/CAS correctness gates.
use super::*;
use std::time::Instant;

/// Leaf-only metadata collection, including the inline/streamed boundary.
/// Fixture construction and the reopened inventory audit are outside timing.
#[tokio::test]
#[ignore = "performance probe; run through benchmark run metadata-collection"]
async fn benchmark_metadata_collection() {
    use crate::digest::{BlobId, Digest};
    use crate::format::{FormatLimits, FormatRegistry};
    use futures::TryStreamExt;
    use std::collections::BTreeSet;
    use std::io::Cursor;

    let counts =
        std::env::var("CASITA_COLLECTION_COUNTS").unwrap_or_else(|_| "256,8192,65536,65537".into());
    let iterations: usize = std::env::var("CASITA_COLLECTION_ITERATIONS")
        .unwrap_or_else(|_| "3".into())
        .parse()
        .unwrap();
    assert!(iterations > 0);
    for count in counts.split(',').map(|v| v.parse::<usize>().unwrap()) {
        assert!(count > 0);
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        let mut revision = store.snapshot().await.unwrap().revision();
        let mut retained = BTreeSet::new();
        for first in (0..count).step_by(1024) {
            let mut mutation = MetadataMutation::new();
            for index in first..(first + 1024).min(count) {
                let bytes = (index as u64).to_le_bytes();
                let key = ObjectKey::blob(BlobId::new(Digest::hash(&bytes)));
                let object = FormatRegistry::builtin()
                    .verify(&key, &mut Cursor::new(bytes), &FormatLimits::default())
                    .await
                    .unwrap();
                retained.insert(key);
                mutation.add_object(object);
            }
            revision = store.commit(&revision, mutation).await.unwrap().revision;
        }
        assert_eq!(retained.len(), count);
        let retained = Arc::new(retained);
        let mut samples = Vec::new();
        for iteration in 0..=iterations {
            let mutation = MetadataMutation::install_retained_source(retained.clone());
            let started = Instant::now();
            let committed = store.commit(&revision, mutation).await.unwrap();
            let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap();
            assert_eq!(committed.objects_removed, 0);
            revision = committed.revision;
            samples.push(serde_json::json!({
                "iteration": iteration, "warm": iteration > 0, "nanos": nanos
            }));
        }
        drop(store);
        let reopened = TursoMetadataStore::open(&path).await.unwrap();
        let snapshot = reopened.snapshot().await.unwrap();
        assert_eq!(snapshot.revision(), revision);
        let actual = snapshot
            .objects_unordered()
            .map_ok(|object| object.key().clone())
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap();
        assert_eq!(&actual, retained.as_ref());
        println!(
            "collection_sample {}",
            serde_json::json!({
                "count": count, "iterations": iterations, "samples": samples,
                "correctness": "zero removals, exact reopened inventory and revision"
            })
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "performance probe; run through benchmark run state-publication"]
async fn benchmark_state_publication() {
    let iterations: usize = std::env::var("CASITA_STATE_BENCH_ITERATIONS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(100);
    let writers: usize = std::env::var("CASITA_STATE_BENCH_WRITERS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(4);
    assert!(iterations > 0 && writers > 1);
    let directory = tempfile::tempdir().unwrap();
    let stores: Vec<(&str, Arc<dyn MetadataStore>)> = vec![
        ("memory", Arc::new(MemoryMetadataStore::new().unwrap())),
        (
            "turso",
            Arc::new(
                TursoMetadataStore::open(directory.path().join("state.sqlite"))
                    .await
                    .unwrap(),
            ),
        ),
    ];
    for (backend, store) in stores {
        let retained = store.snapshot().await.unwrap();
        let retained_revision = retained.revision();
        let started = Instant::now();
        for _ in 0..iterations {
            assert_eq!(
                store.snapshot().await.unwrap().revision(),
                retained_revision
            );
        }
        println!(
            "state_{backend}_snapshot_nanos {}",
            started.elapsed().as_nanos() / iterations as u128
        );
        let started = Instant::now();
        for _ in 0..iterations {
            let revision = store.snapshot().await.unwrap().revision();
            let barrier = Arc::new(tokio::sync::Barrier::new(writers));
            let mut tasks = Vec::new();
            for _ in 0..writers {
                let store = store.clone();
                let barrier = barrier.clone();
                tasks.push(tokio::spawn(async move {
                    barrier.wait().await;
                    store.commit(&revision, MetadataMutation::new()).await
                }));
            }
            let mut winners = 0;
            for task in tasks {
                match task.await.unwrap() {
                    Ok(_) => winners += 1,
                    Err(MetadataError::StaleRevision { .. }) => {}
                    Err(error) => panic!("unexpected publication failure: {error}"),
                }
            }
            assert_eq!(winners, 1, "exact revision CAS must have one winner");
            assert_eq!(retained.revision(), retained_revision);
        }
        println!(
            "state_{backend}_competing_round_nanos {}",
            started.elapsed().as_nanos() / iterations as u128
        );
        let started = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..writers {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                let mut conflicts = 0;
                for _ in 0..iterations {
                    loop {
                        let revision = store.snapshot().await.unwrap().revision();
                        match store.commit(&revision, MetadataMutation::new()).await {
                            Ok(_) => break,
                            Err(MetadataError::StaleRevision { .. }) => conflicts += 1,
                            Err(error) => panic!("unexpected publication failure: {error}"),
                        }
                    }
                }
                conflicts
            }));
        }
        let mut conflicts = 0;
        for task in tasks {
            conflicts += task.await.unwrap();
        }
        println!(
            "state_{backend}_independent_commit_nanos {}",
            started.elapsed().as_nanos() / (iterations * writers) as u128
        );
        println!("state_{backend}_conflicts {conflicts}");
        assert_eq!(retained.revision(), retained_revision);
    }
    println!("state_iterations {iterations}");
    println!("state_writers {writers}");
}

/// What a drive-cache flush per commit would cost: Turso commit latency with
/// the production sync (`fsync`) and with `PRAGMA fullfsync` (`full`,
/// `F_FULLFSYNC` on Apple platforms, the only ones where the modes differ),
/// from empty commits to batches that amortize the flush. Casita flushes once
/// before deletions instead (`blob::deletion_barrier`). A reopened inventory
/// audit gates each mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "performance probe; run through benchmark run metadata-durability"]
async fn benchmark_metadata_commit_durability() {
    use crate::digest::{BlobId, Digest};
    use crate::format::{FormatLimits, FormatRegistry};
    use futures::TryStreamExt;
    use std::collections::BTreeSet;
    use std::io::Cursor;

    let iterations: usize = std::env::var("CASITA_METADATA_DURABILITY_BENCH_ITERATIONS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(20);
    let sizes: Vec<usize> = std::env::var("CASITA_METADATA_DURABILITY_BENCH_OBJECTS")
        .unwrap_or_else(|_| "0,1,1024,16384".into())
        .split(',')
        .map(|v| v.parse().unwrap())
        .collect();
    assert!(iterations > 0 && !sizes.is_empty());
    for (mode, drive_cache_flush) in [("fsync", false), ("full", true)] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.sqlite");
        let store = TursoMetadataStore::open(&path).await.unwrap();
        let setting = if drive_cache_flush { "ON" } else { "OFF" };
        let reported = store
            .database()
            .write(move |connection| {
                Box::pin(async move {
                    connection
                        .execute_batch(&format!("PRAGMA fullfsync = {setting};"))
                        .await?;
                    let mut rows = connection.query("PRAGMA fullfsync", ()).await?;
                    Ok(rows
                        .next()
                        .await?
                        .ok_or("no fullfsync row")?
                        .get::<i64>(0)?)
                })
            })
            .await
            .unwrap();
        assert_eq!(reported, i64::from(drive_cache_flush));
        let mut revision = store.snapshot().await.unwrap().revision();
        let mut expected = BTreeSet::new();
        let mut next_object = 0_u64;
        for &size in &sizes {
            let mut nanos = 0_u128;
            for _ in 0..iterations {
                let mut mutation = MetadataMutation::new();
                for _ in 0..size {
                    let bytes = next_object.to_le_bytes();
                    next_object += 1;
                    let key = ObjectKey::blob(BlobId::new(Digest::hash(&bytes)));
                    let object = FormatRegistry::builtin()
                        .verify(&key, &mut Cursor::new(bytes), &FormatLimits::default())
                        .await
                        .unwrap();
                    expected.insert(key);
                    mutation.add_object(object);
                }
                let started = Instant::now();
                let committed = store.commit(&revision, mutation).await.unwrap();
                nanos += started.elapsed().as_nanos();
                assert_eq!(committed.objects_inserted, size);
                revision = committed.revision;
            }
            println!(
                "metadata_durability_{mode}_objects_{size}_commit_nanos {}",
                nanos / iterations as u128
            );
        }
        drop(store);
        let reopened = TursoMetadataStore::open(&path).await.unwrap();
        let snapshot = reopened.snapshot().await.unwrap();
        assert_eq!(snapshot.revision(), revision);
        let actual = snapshot
            .objects_unordered()
            .map_ok(|object| object.key().clone())
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap();
        assert_eq!(actual, expected, "{mode}: reopened inventory");
    }
    println!("metadata_durability_iterations {iterations}");
}

/// Snapshot acquisition/drop and commits while an older revision is retained.
/// The deep-copy variant reproduces the old snapshot algorithm in this binary.
#[tokio::test]
#[ignore = "performance probe; run through benchmark run memory-snapshots"]
async fn benchmark_memory_snapshots() {
    use std::hint::black_box;
    let count: usize = std::env::var("CASITA_SNAPSHOT_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let iterations: u32 = std::env::var("CASITA_SNAPSHOT_ITERATIONS")
        .unwrap()
        .parse()
        .unwrap();
    let variant = std::env::var("CASITA_SNAPSHOT_VARIANT").unwrap();
    assert!(count > 0 && iterations > 0);
    assert!(matches!(variant.as_str(), "shared" | "deep-copy"));
    let store = MemoryMetadataStore::new().unwrap();
    let mut mutation = MetadataMutation::new();
    let name = RootName::try_from("benchmark/0").unwrap();
    let mut first = None;
    for index in 0..count {
        let object = super::tests::blob(&(index as u64).to_le_bytes()).await;
        let key = object.record().key().clone();
        first.get_or_insert_with(|| key.clone());
        mutation.add_object(object);
        mutation.set_root(
            RootName::try_from(format!("benchmark/{index}")).unwrap(),
            key.clone(),
        );
        mutation.mark_validated_closures([key]);
    }
    mutation.set_payload_catalog(vec![7; count]);
    store
        .commit(&store.snapshot().await.unwrap().revision(), mutation)
        .await
        .unwrap();
    let held = store.snapshot().await.unwrap();
    let original_revision = held.revision();
    let key = first.unwrap();
    // The historical branch clones materialized std BTreeMaps. Construct
    // that template before timing so it remains the old deep-copy algorithm
    // even when the production snapshot uses a different index representation.
    let deep_template = (variant == "deep-copy").then(|| {
        let state = store.state.lock().unwrap();
        (
            state
                .births
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect::<BTreeMap<_, _>>(),
            state
                .objects
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
            state
                .roots
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
            state.validated.iter().cloned().collect::<BTreeSet<_>>(),
        )
    });
    let started = Instant::now();
    for _ in 0..iterations {
        if variant == "shared" {
            let snapshot = store.snapshot().await.unwrap();
            assert_eq!(black_box(&snapshot).revision(), original_revision);
            drop(snapshot);
        } else {
            let state = store.state.lock().unwrap();
            let snapshot = (
                state.records.clone(),
                state.revision,
                state.generation,
                deep_template.as_ref().unwrap().clone(),
                state
                    .payload_catalog
                    .as_ref()
                    .map(|value| (**value).clone()),
            );
            assert_eq!(black_box(&snapshot).1, original_revision);
            drop(snapshot);
        }
    }
    let snapshot_nanos = started.elapsed().as_nanos() / u128::from(iterations);
    let mut revision = original_revision;
    let started = Instant::now();
    for index in 0..iterations {
        let mut mutation = MetadataMutation::new();
        if index.is_multiple_of(2) {
            mutation.remove_root(name.clone());
        } else {
            mutation.set_root(name.clone(), key.clone());
        }
        revision = store.commit(&revision, mutation).await.unwrap().revision;
    }
    let publish_nanos = started.elapsed().as_nanos() / u128::from(iterations);
    let current = store.snapshot().await.unwrap();
    assert_ne!(current.revision(), original_revision);
    assert_eq!(
        current.root(&name).await.unwrap(),
        if iterations.is_multiple_of(2) {
            Some(key.clone())
        } else {
            None
        }
    );
    assert_eq!(held.revision(), original_revision);
    assert_eq!(held.root(&name).await.unwrap(), Some(key.clone()));
    assert_eq!(
        held.validated_closures(std::slice::from_ref(&key))
            .await
            .unwrap(),
        vec![true]
    );
    assert!(held.object(&key).await.unwrap().is_some());
    assert_eq!(held.payload_catalog(), Some(vec![7; count].as_slice()));
    assert_eq!(futures::StreamExt::count(current.objects()).await, count);
    println!(
        "memory_snapshot_sample {}",
        serde_json::json!({
            "count": count, "iterations": iterations, "variant": variant,
            "snapshot_nanos": snapshot_nanos, "publish_nanos": publish_nanos,
            "correctness": "retained revision, object, root, validation and catalog unchanged; exact published root and inventory"
        })
    );
}

#[tokio::test]
#[ignore = "performance probe; run through benchmark run memory-publication"]
async fn benchmark_memory_publication() {
    use crate::NamespaceId;
    use futures::TryStreamExt;
    let count: usize = std::env::var("CASITA_PUBLICATION_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let iterations: u32 = std::env::var("CASITA_PUBLICATION_ITERATIONS")
        .unwrap()
        .parse()
        .unwrap();
    let kind = std::env::var("CASITA_PUBLICATION_KIND").unwrap();
    let readers = std::env::var("CASITA_PUBLICATION_READERS").unwrap() == "held";
    assert!(count > 0 && iterations > 0);
    let store = MemoryMetadataStore::new().unwrap();
    let mut seed = MetadataMutation::new();
    let mut keys = BTreeSet::new();
    for index in 0..count {
        let object = super::tests::blob(&(index as u64).to_le_bytes()).await;
        let key = object.record().key().clone();
        seed.add_object(object);
        seed.set_root(
            RootName::try_from(format!("benchmark/{index}")).unwrap(),
            key.clone(),
        );
        seed.mark_validated_closures([key.clone()]);
        seed.records.insert(
            MetadataKey::new(
                NamespaceId::try_from("benchmark.v1").unwrap(),
                format!("record/{index}"),
            ),
            Some(bytes::Bytes::from_static(b"initial")),
        );
        keys.insert(key);
    }
    seed.set_payload_catalog(vec![7; count]);
    store
        .commit(&store.snapshot().await.unwrap().revision(), seed)
        .await
        .unwrap();
    let original = store.snapshot().await.unwrap();
    let watermark = original.generation().unwrap();
    let mut revision = original.revision();
    let original = readers.then_some(original);
    let name = RootName::try_from("benchmark/0").unwrap();
    let key = super::tests::blob(&0_u64.to_le_bytes())
        .await
        .record()
        .key()
        .clone();
    let record_key = MetadataKey::new(NamespaceId::try_from("benchmark.v1").unwrap(), "record/0");
    let mut nanos = 0;
    for index in 0..iterations {
        let observed = if readers {
            Some(store.snapshot().await.unwrap())
        } else {
            None
        };
        let mut mutation = MetadataMutation::new();
        match kind.as_str() {
            "root" => {
                if index.is_multiple_of(2) {
                    mutation.remove_root(name.clone());
                } else {
                    mutation.set_root(name.clone(), key.clone());
                }
            }
            "record" => {
                mutation.records.insert(
                    record_key.clone(),
                    Some(bytes::Bytes::copy_from_slice(&index.to_le_bytes())),
                );
            }
            "catalog" => {
                mutation.set_payload_catalog(index.to_le_bytes().to_vec());
            }
            "object" => {
                let object =
                    super::tests::blob(&(count as u64 + u64::from(index)).to_le_bytes()).await;
                keys.insert(object.record().key().clone());
                mutation.add_object(object);
            }
            "idempotent" => {
                mutation.add_object(super::tests::blob(&0_u64.to_le_bytes()).await);
                mutation.set_root(name.clone(), key.clone());
                mutation.mark_validated_closures([key.clone()]);
            }
            "collection" => {
                mutation = MetadataMutation::install_retained_objects(keys.clone());
            }
            _ => panic!("unknown publication kind"),
        }
        let started = Instant::now();
        revision = store.commit(&revision, mutation).await.unwrap().revision;
        drop(observed);
        nanos += started.elapsed().as_nanos();
    }
    let current = store.snapshot().await.unwrap();
    assert_eq!(current.revision(), revision);
    assert_eq!(
        current
            .objects()
            .map_ok(|v| v.key().clone())
            .try_collect::<BTreeSet<_>>()
            .await
            .unwrap(),
        keys
    );
    assert_eq!(
        current.root(&name).await.unwrap(),
        if kind == "root" && !iterations.is_multiple_of(2) {
            None
        } else {
            Some(key.clone())
        }
    );
    let expected_catalog = if kind == "catalog" {
        (iterations - 1).to_le_bytes().to_vec()
    } else {
        vec![7; count]
    };
    assert_eq!(current.payload_catalog(), Some(expected_catalog.as_slice()));
    assert_eq!(
        current
            .get(std::slice::from_ref(&record_key))
            .await
            .unwrap(),
        vec![Some(if kind == "record" {
            bytes::Bytes::copy_from_slice(&(iterations - 1).to_le_bytes())
        } else {
            bytes::Bytes::from_static(b"initial")
        })]
    );
    assert_eq!(
        current
            .objects_created_through(watermark)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        count
    );
    if let Some(original) = original {
        assert_eq!(original.root(&name).await.unwrap(), Some(key));
        assert_eq!(
            original.get(&[record_key]).await.unwrap(),
            vec![Some(bytes::Bytes::from_static(b"initial"))]
        );
        assert_eq!(original.payload_catalog(), Some(vec![7; count].as_slice()));
        assert_eq!(
            original
                .objects()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .len(),
            count
        );
    }
    println!(
        "memory_publication_sample {}",
        serde_json::json!({
            "count": count, "iterations": iterations, "kind": kind, "readers": if readers { "held" } else { "none" }, "commit_nanos": nanos / u128::from(iterations),
            "correctness": "exact inventory, roots, records, catalog and birth generations; held snapshots unchanged"
        })
    );
}

/// Counterweights to commit latency: ordered reads, collection and last-owner drop.
#[tokio::test]
#[ignore = "performance probe; run through benchmark run memory-index-lifecycle"]
async fn benchmark_memory_index_lifecycle() {
    use futures::TryStreamExt;
    let count: usize = std::env::var("CASITA_INDEX_COUNT")
        .unwrap()
        .parse()
        .unwrap();
    let percent: usize = std::env::var("CASITA_INDEX_REMOVE_PERCENT")
        .unwrap()
        .parse()
        .unwrap();
    let readers = std::env::var("CASITA_INDEX_READERS").unwrap() == "held";
    assert!(count > 0 && percent <= 100);
    let removed = count * percent / 100;
    let store = MemoryMetadataStore::new().unwrap();
    let namespace = crate::NamespaceId::try_from("index.v1").unwrap();
    let mut seed = MetadataMutation::new();
    let mut all = BTreeSet::new();
    let mut retained = BTreeSet::new();
    let mut records = BTreeMap::new();
    for index in 0..count {
        let object = super::tests::blob(&(index as u64).to_le_bytes()).await;
        let key = object.record().key().clone();
        all.insert(key.clone());
        if index >= removed {
            retained.insert(key.clone());
            seed.set_root(
                RootName::try_from(format!("root/{index:08}")).unwrap(),
                key.clone(),
            );
        }
        seed.mark_validated_closures([key]);
        seed.add_object(object);
        let record_key = MetadataKey::new(namespace.clone(), format!("record/{index:08}"));
        let value = bytes::Bytes::copy_from_slice(&(index as u64).to_le_bytes());
        records.insert(record_key.clone(), value.clone());
        seed.records.insert(record_key, Some(value));
    }
    let revision = store.snapshot().await.unwrap().revision();
    let started = Instant::now();
    let seeded = store.commit(&revision, seed).await.unwrap();
    let seed_nanos = started.elapsed().as_nanos();
    assert_eq!(seeded.objects_inserted, count);
    let snapshot = store.snapshot().await.unwrap();
    let watermark = snapshot.generation().unwrap();
    let started = Instant::now();
    for key in &all {
        assert_eq!(snapshot.object(key).await.unwrap().unwrap().key(), key);
    }
    for chunk in records.keys().cloned().collect::<Vec<_>>().chunks(128) {
        let got = snapshot.get(chunk).await.unwrap();
        assert_eq!(
            got,
            chunk
                .iter()
                .map(|k| Some(records[k].clone()))
                .collect::<Vec<_>>()
        );
    }
    let lookup_nanos = started.elapsed().as_nanos();
    let prefix = MetadataKey::new(namespace, "record/");
    let mut after = None;
    let mut scanned = Vec::new();
    let started = Instant::now();
    loop {
        let page = snapshot.scan(&prefix, after.as_deref(), 128).await.unwrap();
        if page.is_empty() {
            break;
        }
        // The backend returns up to limit + 1 for continuation detection.
        for record in page.into_iter().take(128) {
            after = Some(record.key.key.clone());
            scanned.push((record.key, record.value));
        }
    }
    let scan_nanos = started.elapsed().as_nanos();
    assert_eq!(scanned, records.into_iter().collect::<Vec<_>>());
    let original = readers.then_some(snapshot);
    let mutation = MetadataMutation::install_retained_objects(retained.clone());
    let started = Instant::now();
    let collected = store.commit(&seeded.revision, mutation).await.unwrap();
    let collection_nanos = started.elapsed().as_nanos();
    assert_eq!(collected.objects_removed, removed);
    let current = store.snapshot().await.unwrap();
    let inventory = current
        .objects()
        .map_ok(|r| r.key().clone())
        .try_collect::<BTreeSet<_>>()
        .await
        .unwrap();
    assert_eq!(inventory, retained);
    assert_eq!(
        current
            .objects_created_through(watermark)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        retained.len()
    );
    let keys = all.iter().cloned().collect::<Vec<_>>();
    assert_eq!(
        current.validated_closures(&keys).await.unwrap(),
        keys.iter()
            .map(|key| retained.contains(key))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        current.roots().try_collect::<Vec<_>>().await.unwrap().len(),
        retained.len()
    );
    if let Some(original) = &original {
        assert_eq!(original.revision(), seeded.revision);
        assert_eq!(
            original
                .objects()
                .map_ok(|r| r.key().clone())
                .try_collect::<BTreeSet<_>>()
                .await
                .unwrap(),
            all
        );
        assert!(
            original
                .validated_closures(&keys)
                .await
                .unwrap()
                .iter()
                .all(|v| *v)
        );
    }
    let started = Instant::now();
    drop(current);
    drop(original);
    drop(store);
    let drop_nanos = started.elapsed().as_nanos();
    println!(
        "memory_index_sample {}",
        serde_json::json!({
            "count": count, "remove_percent": percent, "readers": if readers { "held" } else { "none" },
            "seed_nanos": seed_nanos, "lookup_nanos": lookup_nanos, "scan_nanos": scan_nanos,
            "collection_nanos": collection_nanos, "drop_nanos": drop_nanos,
            "correctness": "exact lookup and paginated order; retained inventory, roots, validation and births; original snapshot unchanged"
        })
    );
}
