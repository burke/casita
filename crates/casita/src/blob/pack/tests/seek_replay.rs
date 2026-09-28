//! Permanent measured-range replay through the production packed ChunkSource.
use super::*;
use crate::blob::BlobReader;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

async fn run_case(case: &str, capacity: usize, cycles: usize, budget: usize) -> Value {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../../../benchmarks/fixtures/native-launch-ranges.json"
    ))
    .unwrap();
    let reader_count = match case {
        "shared-below" => 15,
        "shared-at" => 16,
        "shared-above" => 17,
        _ => 1,
    };
    let phase_hot_chunks = match case {
        "phase-demand-below" | "phase-demand-at" | "phase-demand-above" => 1,
        "phase-history-below" => 63,
        "phase-history-at" => 64,
        "phase-history-above" => 65,
        _ => 8,
    };
    let phase_chunk_bytes = if case.starts_with("phase-history-") {
        16384
    } else {
        256 * 1024
    };
    let phase_hot_bytes = phase_hot_chunks * phase_chunk_bytes;
    let phase_warm_size = match case {
        "phase-demand-below" | "phase-demand-at" | "phase-demand-above" => {
            Some(crate::blob::chunked_reader::DECODED_CACHE_BYTES)
        }
        "phase-history-below" | "phase-history-at" | "phase-history-above" => {
            Some(crate::blob::chunked_reader::DECODED_CACHE_BYTES)
        }
        "phase-below" => Some(crate::blob::chunked_reader::DECODED_CACHE_BYTES - 1),
        "phase-at" => Some(crate::blob::chunked_reader::DECODED_CACHE_BYTES),
        "phase-above" => Some(crate::blob::chunked_reader::DECODED_CACHE_BYTES + 1),
        _ => None,
    };
    let size = match case {
        _ if phase_warm_size.is_some() => phase_hot_bytes + phase_warm_size.unwrap(),
        "shared-below" | "shared-at" | "shared-above" => {
            crate::blob::chunked_reader::DECODED_CACHE_BYTES
        }
        "entries-below" => 63 * 16384,
        "entries-at" => 64 * 16384,
        "entries-above" | "entries-above-repeated" => 65 * 16384,
        "launch" | "sequential" | "one-seek" | "two-seeks" => {
            fixture["tool_size"].as_u64().unwrap() as usize
        }
        "chunk-below" | "working-below" | "parked-below" => {
            crate::blob::chunked_reader::DECODED_CACHE_BYTES - 1
        }
        "chunk-at" | "working-at" | "parked-at" => crate::blob::chunked_reader::DECODED_CACHE_BYTES,
        "chunk-above" | "working-above" | "parked-above" => {
            crate::blob::chunked_reader::DECODED_CACHE_BYTES + 1
        }
        "parked-working" => 3 * crate::blob::chunked_reader::DECODED_CACHE_BYTES,
        _ => panic!("unknown case"),
    };
    let mut data = vec![0; size];
    blake3::Hasher::new()
        .update(b"decoded-seek-replay")
        .finalize_xof()
        .fill(&mut data);
    let mut input_path = None;
    if matches!(case, "launch" | "sequential" | "one-seek" | "two-seeks")
        && let Ok(path) = std::env::var("CASITA_SEEK_INPUT")
    {
        data = std::fs::read(&path).unwrap();
        assert_eq!(data.len(), size);
        input_path = Some(path);
    }
    let pieces: Vec<(usize, usize)> = if let Some(warm_size) = phase_warm_size {
        // Put the initial working set last so read-ahead cannot populate the
        // later working set before the phase transition.
        (0..phase_hot_bytes)
            .step_by(phase_chunk_bytes)
            .map(|at| (at, phase_chunk_bytes))
            .chain([(phase_hot_bytes, warm_size)])
            .collect()
    } else if case.starts_with("chunk-")
        || case.starts_with("shared-")
        || (case.starts_with("parked-") && case != "parked-working")
    {
        vec![(0, data.len())]
    } else if case.starts_with("entries-") {
        (0..data.len())
            .step_by(16384)
            .map(|at| (at, 16384))
            .collect()
    } else if case.starts_with("working-") || case == "parked-working" {
        (0..data.len())
            .step_by(256 * 1024)
            .map(|at| (at, (data.len() - at).min(256 * 1024)))
            .collect()
    } else {
        fastcdc::v2020::FastCDC::new(&data, 128 * 1024, 256 * 1024, 512 * 1024)
            .map(|c| (c.offset, c.length))
            .collect()
    };
    let directory = tempfile::tempdir().unwrap();
    // A LocalFileSystem cannot condition catalog updates, so a local packed
    // store publishes through the same catalog lock production uses.
    let filesystem =
        object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap();
    let durability =
        crate::blob::local_durability::LocalDurability::new(filesystem.clone(), directory.path())
            .unwrap();
    let packed = PackedChunks::open_with_cache_and_durability(
        Arc::new(filesystem),
        Path::default(),
        4 * 1024 * 1024,
        8 * 1024 * 1024,
        Some(durability),
    )
    .await
    .unwrap();
    let mut chunks = Vec::new();
    for &(at, len) in &pieces {
        let bytes = &data[at..at + len];
        let chunk = ChunkMeta {
            digest: ChunkId::new(blake3::hash(bytes).into()),
            size: len as u64,
        };
        packed
            .put(
                chunk.clone(),
                Bytes::from(zstd::encode_all(bytes, 0).unwrap()),
            )
            .await
            .unwrap();
        chunks.push(chunk);
    }
    packed.flush().await.unwrap();
    // Keep compressed cache policy identical; both modes start with it warm.
    for chunk in &chunks {
        packed.get(&chunk.digest).await.unwrap().unwrap();
    }
    let frozen = packed.freeze_manifest(&chunks).await.unwrap().unwrap();
    let profile = Arc::new(SeekProfile::default());
    let source = Arc::new(Source {
        context: Arc::new(Context {
            packed: packed.reader(),
            plan: ReadPlan {
                chunks: chunks.clone(),
                frozen,
                _pin: None,
            },
            decode: ByteBudget::new(budget),
        }),
        cursor: Mutex::new(Cursor {
            at: 0,
            frames: None,
        }),
        profile: Some(profile.clone()),
    });
    let owner = Arc::downgrade(&source.context);
    let mut inputs: Vec<_> = (0..reader_count)
        .map(|_| {
            ChunkedReader::with_cache_capacity(
                source.clone(),
                chunks.iter().map(|c| (c.digest, c.size)),
                Some(BlobId::new(blake3::hash(&data).into())),
                capacity,
            )
        })
        .collect();
    drop(source);
    let ranges: Vec<(u64, usize)> = if case.starts_with("parked-") {
        (0..data.len())
            .step_by(8192)
            .map(|at| (at as u64, (data.len() - at).min(8192)))
            .collect()
    } else if case == "launch" {
        serde_json::from_value(fixture["ranges"].clone()).unwrap()
    } else if phase_warm_size.is_some() {
        pieces[..phase_hot_chunks]
            .iter()
            .map(|(at, _)| ((*at + 1) as u64, 4096))
            .collect()
    } else if case == "one-seek" {
        vec![(1, 4096)]
    } else if case == "two-seeks" {
        vec![(1, 4096), (0, 4096)]
    } else if case == "sequential" {
        vec![(0, data.len())]
    } else if case.starts_with("chunk-") || case.starts_with("shared-") {
        vec![(1, 4096), (0, 4096)]
    } else {
        pieces
            .iter()
            .map(|(at, len)| {
                let skip = usize::from(*len > 1);
                ((*at + skip) as u64, 4096.min(len - skip))
            })
            .collect()
    };
    let ranges = if case == "entries-above-repeated" {
        ranges
            .into_iter()
            .flat_map(|(offset, length)| [(offset, length), (offset + 1, length)])
            .collect()
    } else {
        ranges
    };
    let mut warm_cache_bytes = 0;
    let mut warm_elapsed_ns = 0;
    if let Some(warm_size) = phase_warm_size {
        let started = Instant::now();
        for offset in [phase_hot_bytes as u64 + 1, phase_hot_bytes as u64] {
            inputs[0].seek(io::SeekFrom::Start(offset)).await.unwrap();
            let mut actual = vec![0; 4096];
            inputs[0].read_exact(&mut actual).await.unwrap();
            assert_eq!(actual, data[offset as usize..offset as usize + 4096]);
        }
        warm_elapsed_ns = started.elapsed().as_nanos() as u64;
        warm_cache_bytes = inputs[0].decoded_cache_usage().0;
        assert_eq!(
            warm_cache_bytes,
            if warm_size <= capacity { warm_size } else { 0 }
        );
    }
    let decodes_before = profile.calls.load(Ordering::Relaxed);
    let bytes_before = profile.decoded_bytes.load(Ordering::Relaxed);
    let fetch_before = profile.fetch_ns.load(Ordering::Relaxed);
    let admission_before = profile.admission_ns.load(Ordering::Relaxed);
    let decode_before = profile.decode_ns.load(Ordering::Relaxed);
    packed.reset_read_stats();
    let started = Instant::now();
    let mut returned = 0;
    for _ in 0..cycles {
        for input in &mut inputs {
            for &(offset, length) in &ranges {
                input.seek(io::SeekFrom::Start(offset)).await.unwrap();
                let wanted = length.min(data.len() - offset as usize);
                let mut actual = vec![0; wanted];
                input.read_exact(&mut actual).await.unwrap();
                assert_eq!(actual, &data[offset as usize..offset as usize + wanted]);
                returned += actual.len();
                if case.starts_with("parked-") {
                    input.park().await;
                }
            }
        }
        // The sequential control creates no artificial rewind within a reader.
        if case == "sequential" || case.starts_with("parked-") {
            break;
        }
    }
    let elapsed = started.elapsed().as_nanos() as u64;
    let cache = inputs
        .iter()
        .map(ChunkedReader::decoded_cache_usage)
        .fold((0, 0), |sum, next| (sum.0 + next.0, sum.1 + next.1));
    let shared_used = crate::blob::chunked_reader::shared_decoded_cache_usage();
    assert!(cache.0 <= capacity * reader_count && cache.1 <= 64 * reader_count);
    assert!(shared_used <= crate::blob::chunked_reader::SHARED_DECODED_CACHE_BYTES);
    if case.starts_with("shared-") {
        assert_eq!(
            cache.0,
            (capacity * reader_count).min(crate::blob::chunked_reader::SHARED_DECODED_CACHE_BYTES)
        );
    }
    if matches!(case, "sequential" | "one-seek") {
        assert_eq!(cache.0, 0);
    }
    let stats = packed.read_stats();
    drop(inputs);
    crate::flush_repository_leases().await.unwrap();
    assert!(
        owner.upgrade().is_none(),
        "reader retained packed source after release"
    );
    if case.starts_with("shared-") {
        assert_eq!(crate::blob::chunked_reader::shared_decoded_cache_usage(), 0);
    }
    json!({"phase_hot_chunks":phase_hot_chunks,"phase_warm_bytes":phase_warm_size,"warm_cache_bytes":warm_cache_bytes,
        "warm_elapsed_ns":warm_elapsed_ns,"measured_decode_calls":profile.calls.load(Ordering::Relaxed)-decodes_before,
        "reader_count":reader_count,"shared_cache_bytes":shared_used,"case":case,"capacity":capacity,"cycles":cycles,"elapsed_ns":elapsed,
        "source":input_path,"input_blake3":blake3::hash(&data).to_hex().to_string(),"input_bytes":data.len(),
        "chunk_sizes":pieces.iter().map(|p|p.1).collect::<Vec<_>>(),"ranges":ranges,"returned_bytes":returned,
        "decode_calls":profile.calls.load(Ordering::Relaxed)-decodes_before,"decoded_bytes":profile.decoded_bytes.load(Ordering::Relaxed)-bytes_before,
        "fetch_ns":profile.fetch_ns.load(Ordering::Relaxed)-fetch_before,"admission_ns":profile.admission_ns.load(Ordering::Relaxed)-admission_before,
        "decode_ns":profile.decode_ns.load(Ordering::Relaxed)-decode_before,"chunk_range_requests":stats.chunk_range_requests,
        "cache_bytes":cache.0,"cache_entries":cache.1,"correctness":"passed","release":"passed"})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "permanent benchmark: benchmark decoded-seek-replay"]
async fn benchmark_seek_replay() {
    let case = std::env::var("CASITA_SEEK_CASE").unwrap();
    let capacity = std::env::var("CASITA_SEEK_CAPACITY")
        .unwrap()
        .parse()
        .unwrap();
    let cycles = std::env::var("CASITA_SEEK_CYCLES")
        .unwrap()
        .parse()
        .unwrap();
    let result = run_case(&case, capacity, cycles, 64 * 1024 * 1024).await;
    println!("seek_replay_sample {result}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decoded_reuse_releases_permits_with_tiny_budget() {
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        run_case("working-at", 2 * 1024 * 1024, 3, 1),
    )
    .await
    .unwrap();
    assert_eq!(result["correctness"], "passed");
}
