//! Bounded-memory catalog-run staging and merging.
//!
//! The on-disk run already contains the three sorted views needed by a merge:
//! pack state in the exact delta, manifest mutations in the delta, and chunk
//! locations in authenticated query blocks. Staging immutable inputs into
//! temporary files lets a carry merge those views without constructing an
//! [`Index`] proportional to the run size.

use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};

use bytes::Bytes;
use tempfile::NamedTempFile;

use super::delta::{CatalogRunQueryRef, CatalogRunRef, DELTA_INVENTORY};
use super::run::{
    CATALOG_RUN_DOMAIN, CATALOG_RUN_MAGIC_V2, CatalogRunChunkBlockRef, CatalogRunQueryIndex,
    RUN_QUERY_TRAILER_BYTES, decode_catalog_run_query_tail, encode_run_chunk_block,
    encode_run_query_tail,
};
use super::*;

const RUN_HEADER_BYTES: u64 = 8 + 8 + 8 + DIGEST_LEN as u64 + 8;
const CHUNK_RECORD_BYTES: u64 = (DIGEST_LEN * 2 + 8 * 4) as u64;
// The streamed forms are byte-identical to the in-memory codecs.
const DELTA_MAGIC: [u8; 8] = super::delta::INDEX_DELTA_MAGIC_V1;
const CHECKPOINT_MAGIC: [u8; 8] = INDEX_CHECKPOINT_MAGIC_V1;

#[derive(Debug, Clone, Copy)]
struct FixedSection {
    offset: u64,
    count: u64,
}

#[derive(Debug, Clone, Copy)]
struct PackSection {
    offset: u64,
    count: u64,
    end: u64,
}

#[derive(Debug)]
struct RunLayout {
    removed_packs: FixedSection,
    removed_manifests: FixedSection,
    packs: PackSection,
    superseded: FixedSection,
    manifests: FixedSection,
    manifests_complete: bool,
}

pub(super) struct StagedCatalogRun {
    file: NamedTempFile,
    pub(super) reference: CatalogRunRef,
    layout: RunLayout,
    query: CatalogRunQueryIndex,
}

pub(super) struct PreparedCatalogRunFile {
    pub(super) file: NamedTempFile,
    pub(super) reference: CatalogRunRef,
}

/// Independent sequential readers over every sorted view in one consolidated
/// run. Keeping the staged file alive makes this portable to platforms that
/// cannot read an unlinked temporary file.
pub(super) struct CatalogRunReaders {
    _file: NamedTempFile,
    chunks: ChunkIter,
    packs: PackIter,
    removed_packs: IdIter,
    superseded: IdIter,
    added_manifests: IdIter,
    removed_manifests: IdIter,
    changed_packs: Vec<PackId>,
    chunk_entries: u64,
}

impl StagedCatalogRun {
    pub(super) fn into_readers(self) -> io::Result<CatalogRunReaders> {
        let chunks = ChunkIter::new(self.file.path(), &self.query)?;
        let packs = PackIter::new(self.file.path(), self.layout.packs)?;
        let removed_packs = IdIter::new(self.file.path(), self.layout.removed_packs)?;
        let superseded = IdIter::new(self.file.path(), self.layout.superseded)?;
        let added_manifests = IdIter::new(self.file.path(), self.layout.manifests)?;
        let removed_manifests = IdIter::new(self.file.path(), self.layout.removed_manifests)?;
        let chunk_entries = self.query.chunks.iter().try_fold(0_u64, |total, block| {
            let payload = block
                .encoded_bytes
                .checked_sub(16)
                .ok_or_else(|| io::Error::other("catalog chunk block is too short"))?;
            if payload % CHUNK_RECORD_BYTES != 0 {
                return Err(io::Error::other("catalog chunk block is misaligned"));
            }
            total
                .checked_add(payload / CHUNK_RECORD_BYTES)
                .ok_or_else(|| io::Error::other("catalog chunk count overflow"))
        })?;
        Ok(CatalogRunReaders {
            _file: self.file,
            chunks,
            packs,
            removed_packs,
            superseded,
            added_manifests,
            removed_manifests,
            changed_packs: self.query.changed_packs,
            chunk_entries,
        })
    }
}

impl CatalogRunReaders {
    pub(super) fn changed_packs(&self) -> &[PackId] {
        &self.changed_packs
    }

    pub(super) fn chunk_entries(&self) -> u64 {
        self.chunk_entries
    }

    pub(super) fn next_chunk(&mut self) -> io::Result<Option<IndexedLocation>> {
        self.chunks.next_entry()
    }

    pub(super) fn next_pack(&mut self) -> io::Result<Option<(PackId, Index)>> {
        let Some((pack, raw)) = self.packs.next_record()? else {
            return Ok(None);
        };
        let mut checkpoint = Vec::with_capacity(8 + DIGEST_LEN + 8 + raw.len() + 25);
        checkpoint.extend_from_slice(&CHECKPOINT_MAGIC);
        checkpoint.extend_from_slice(blake3::hash(DELTA_INVENTORY).as_bytes());
        checkpoint.extend_from_slice(&1_u64.to_le_bytes());
        checkpoint.extend_from_slice(&raw);
        checkpoint.extend_from_slice(&0_u64.to_le_bytes());
        checkpoint.push(0);
        checkpoint.extend_from_slice(&0_u64.to_le_bytes());
        let index = decode_index_checkpoint_without_inventory(&checkpoint)?;
        Ok(Some((pack, index)))
    }

    pub(super) fn next_removed_pack(&mut self) -> io::Result<Option<PackId>> {
        Ok(self.removed_packs.next_digest()?.map(PackId::new))
    }

    pub(super) fn next_superseded(&mut self) -> io::Result<Option<PackId>> {
        Ok(self.superseded.next_digest()?.map(PackId::new))
    }

    pub(super) fn next_added_manifest(&mut self) -> io::Result<Option<BlobId>> {
        Ok(self.added_manifests.next_digest()?.map(BlobId::new))
    }

    pub(super) fn next_removed_manifest(&mut self) -> io::Result<Option<BlobId>> {
        Ok(self.removed_manifests.next_digest()?.map(BlobId::new))
    }
}

fn read_exact_at(file: &mut File, offset: u64, bytes: &mut [u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)
}

fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn advance(at: u64, bytes: u64, limit: u64) -> io::Result<u64> {
    let end = at
        .checked_add(bytes)
        .ok_or_else(|| io::Error::other("catalog run offset overflow"))?;
    if end > limit {
        return Err(io::Error::other("truncated catalog run"));
    }
    Ok(end)
}

fn fixed_section(
    reader: &mut BufReader<File>,
    at: &mut u64,
    limit: u64,
) -> io::Result<FixedSection> {
    reader.seek(SeekFrom::Start(*at))?;
    let count = read_u64(reader)?;
    *at = advance(*at, 8, limit)?;
    let offset = *at;
    *at = advance(
        *at,
        count
            .checked_mul(DIGEST_LEN as u64)
            .ok_or_else(|| io::Error::other("catalog run count overflow"))?,
        limit,
    )?;
    Ok(FixedSection { offset, count })
}

fn skip_pack_record(reader: &mut BufReader<File>, at: &mut u64, limit: u64) -> io::Result<()> {
    *at = advance(*at, DIGEST_LEN as u64 + 8, limit)?;
    reader.seek(SeekFrom::Start(*at))?;
    let footer_len = read_u64(reader)?;
    *at = advance(*at, 8, limit)?;
    *at = advance(*at, footer_len, limit)?;
    reader.seek(SeekFrom::Start(*at))?;
    let bitmap_len = read_u64(reader)?;
    *at = advance(*at, 8, limit)?;
    *at = advance(*at, bitmap_len, limit)?;
    reader.seek(SeekFrom::Start(*at))?;
    let record_count = read_u64(reader)?;
    *at = advance(*at, 8, limit)?;
    *at = advance(
        *at,
        record_count
            .checked_mul(DIGEST_LEN as u64)
            .ok_or_else(|| io::Error::other("catalog tombstone count overflow"))?,
        limit,
    )?;
    Ok(())
}

fn inspect(
    file: &File,
    reference: &CatalogRunRef,
) -> io::Result<(RunLayout, CatalogRunQueryIndex)> {
    let object_len = file.metadata()?.len();
    if object_len != reference.encoded_bytes || object_len < RUN_HEADER_BYTES {
        return Err(io::Error::other("catalog run has the wrong length"));
    }
    let mut header = [0_u8; RUN_HEADER_BYTES as usize];
    let mut raw = file.try_clone()?;
    read_exact_at(&mut raw, 0, &mut header)?;
    if header[..8] != CATALOG_RUN_MAGIC_V2 {
        return Err(io::Error::other(
            "streaming merge requires a queryable catalog run",
        ));
    }
    let first = u64::from_le_bytes(header[8..16].try_into().expect("eight bytes"));
    let last = u64::from_le_bytes(header[16..24].try_into().expect("eight bytes"));
    if first != reference.first_generation || last != reference.last_generation {
        return Err(io::Error::other("catalog run generation mismatch"));
    }
    let delta_len = u64::from_le_bytes(header[56..64].try_into().expect("eight bytes"));
    let delta_end = advance(RUN_HEADER_BYTES, delta_len, object_len)?;
    let expected_checksum = Digest::try_from(&header[24..56]).map_err(io::Error::other)?;
    let mut checksum = blake3::Hasher::new();
    checksum.update(CATALOG_RUN_DOMAIN);
    checksum.update(&first.to_le_bytes());
    checksum.update(&last.to_le_bytes());
    checksum.update(&delta_len.to_le_bytes());
    // This clone is consumed sequentially before any section iterators exist,
    // so its shared Unix cursor cannot race another reader.
    let mut checksum_reader = BufReader::new(file.try_clone()?);
    checksum_reader.seek(SeekFrom::Start(RUN_HEADER_BYTES))?;
    let mut checksum_remaining = delta_len;
    let mut checksum_buffer = vec![0_u8; 1024 * 1024];
    while checksum_remaining != 0 {
        let take = usize::try_from(checksum_remaining.min(checksum_buffer.len() as u64))
            .expect("bounded catalog checksum read");
        checksum_reader.read_exact(&mut checksum_buffer[..take])?;
        checksum.update(&checksum_buffer[..take]);
        checksum_remaining -= take as u64;
    }
    if Digest::from(checksum.finalize()) != expected_checksum {
        return Err(io::Error::other("catalog run checksum mismatch"));
    }
    let mut reader = BufReader::new(file.try_clone()?);
    reader.seek(SeekFrom::Start(RUN_HEADER_BYTES))?;
    let mut magic = [0_u8; 8];
    reader.read_exact(&mut magic)?;
    if magic != DELTA_MAGIC {
        return Err(io::Error::other("invalid streamed catalog delta"));
    }
    let mut at = RUN_HEADER_BYTES + 8;
    let removed_packs = fixed_section(&mut reader, &mut at, delta_end)?;
    let removed_manifests = fixed_section(&mut reader, &mut at, delta_end)?;
    reader.seek(SeekFrom::Start(at))?;
    let patch_len = read_u64(&mut reader)?;
    at = advance(at, 8, delta_end)?;
    let patch_end = advance(at, patch_len, delta_end)?;
    if patch_end != delta_end {
        return Err(io::Error::other("catalog delta patch length mismatch"));
    }
    reader.seek(SeekFrom::Start(at))?;
    reader.read_exact(&mut magic)?;
    if magic != CHECKPOINT_MAGIC {
        return Err(io::Error::other("invalid streamed catalog checkpoint"));
    }
    at = advance(at, 8 + DIGEST_LEN as u64, patch_end)?;
    reader.seek(SeekFrom::Start(at))?;
    let pack_count = read_u64(&mut reader)?;
    at = advance(at, 8, patch_end)?;
    let packs_offset = at;
    for _ in 0..pack_count {
        skip_pack_record(&mut reader, &mut at, patch_end)?;
    }
    let packs = PackSection {
        offset: packs_offset,
        count: pack_count,
        end: at,
    };
    let superseded = fixed_section(&mut reader, &mut at, patch_end)?;
    reader.seek(SeekFrom::Start(at))?;
    let mut complete = [0_u8; 1];
    reader.read_exact(&mut complete)?;
    let manifests_complete = match complete[0] {
        0 => false,
        1 => true,
        _ => return Err(io::Error::other("invalid manifest completeness flag")),
    };
    at = advance(at, 1, patch_end)?;
    let manifests = fixed_section(&mut reader, &mut at, patch_end)?;
    if at != patch_end {
        return Err(io::Error::other("trailing streamed checkpoint bytes"));
    }
    let query_reference = reference
        .query
        .as_ref()
        .ok_or_else(|| io::Error::other("catalog run has no query routing"))?;
    let query_end = query_reference
        .offset
        .checked_add(query_reference.encoded_bytes)
        .ok_or_else(|| io::Error::other("catalog run query range overflow"))?;
    if query_end != object_len
        || query_reference.encoded_bytes > super::run::RUN_QUERY_TAIL_MAX_BYTES as u64
    {
        return Err(io::Error::other("catalog run query range mismatch"));
    }
    let query_len = usize::try_from(query_reference.encoded_bytes)
        .map_err(|_| io::Error::other("catalog run query length overflow"))?;
    let mut query_bytes = vec![0_u8; query_len];
    read_exact_at(&mut raw, query_reference.offset, &mut query_bytes)?;
    if Digest::from(blake3::hash(&query_bytes)) != query_reference.digest
        || query_bytes.get(..query_len.saturating_sub(RUN_QUERY_TRAILER_BYTES))
            != Some(query_reference.routing.as_ref())
    {
        return Err(io::Error::other("catalog run query identity mismatch"));
    }
    let query = decode_catalog_run_query_tail(
        &query_bytes,
        usize::try_from(query_reference.offset)
            .map_err(|_| io::Error::other("catalog run query offset overflow"))?,
        usize::try_from(object_len)
            .map_err(|_| io::Error::other("catalog run object length overflow"))?,
    )?;
    Ok((
        RunLayout {
            removed_packs,
            removed_manifests,
            packs,
            superseded,
            manifests,
            manifests_complete,
        },
        query,
    ))
}

pub(super) fn stage_file(
    file: NamedTempFile,
    reference: CatalogRunRef,
) -> io::Result<StagedCatalogRun> {
    let (layout, query) = inspect(file.as_file(), &reference)?;
    Ok(StagedCatalogRun {
        file,
        reference,
        layout,
        query,
    })
}

/// Stage `bytes` as a scratch merge input. The file lives in the temporary
/// directory only until the merge reads it back, so it is never synced: a crash
/// discards it anyway, and the merged run is published through
/// `LocalDurability` or the object store.
pub(super) fn stage_bytes(bytes: &[u8], reference: CatalogRunRef) -> io::Result<StagedCatalogRun> {
    let mut file = NamedTempFile::new()?;
    file.write_all(bytes)?;
    stage_file(file, reference)
}

struct IdIter {
    reader: BufReader<File>,
    remaining: u64,
}

impl IdIter {
    fn new(path: &std::path::Path, section: FixedSection) -> io::Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        reader.seek(SeekFrom::Start(section.offset))?;
        Ok(Self {
            reader,
            remaining: section.count,
        })
    }

    fn next_digest(&mut self) -> io::Result<Option<Digest>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut bytes = [0_u8; DIGEST_LEN];
        self.reader.read_exact(&mut bytes)?;
        self.remaining -= 1;
        Ok(Some(Digest::from(bytes)))
    }
}

struct PackIter {
    reader: BufReader<File>,
    remaining: u64,
    end: u64,
}

impl PackIter {
    fn new(path: &std::path::Path, section: PackSection) -> io::Result<Self> {
        let mut reader = BufReader::new(File::open(path)?);
        reader.seek(SeekFrom::Start(section.offset))?;
        Ok(Self {
            reader,
            remaining: section.count,
            end: section.end,
        })
    }

    fn next_record(&mut self) -> io::Result<Option<(PackId, Vec<u8>)>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let start = self.reader.stream_position()?;
        let mut id = [0_u8; DIGEST_LEN];
        self.reader.read_exact(&mut id)?;
        let _pack_len = read_u64(&mut self.reader)?;
        let footer_len = read_u64(&mut self.reader)?;
        self.reader
            .seek(SeekFrom::Current(i64::try_from(footer_len).map_err(
                |_| io::Error::other("catalog pack footer is too large"),
            )?))?;
        let bitmap_len = read_u64(&mut self.reader)?;
        self.reader
            .seek(SeekFrom::Current(i64::try_from(bitmap_len).map_err(
                |_| io::Error::other("catalog tombstone bitmap is too large"),
            )?))?;
        let records = read_u64(&mut self.reader)?;
        self.reader.seek(SeekFrom::Current(
            i64::try_from(
                records
                    .checked_mul(DIGEST_LEN as u64)
                    .ok_or_else(|| io::Error::other("catalog tombstone records overflow"))?,
            )
            .map_err(|_| io::Error::other("catalog tombstone records are too large"))?,
        ))?;
        let end = self.reader.stream_position()?;
        if end > self.end {
            return Err(io::Error::other("catalog pack record exceeds its section"));
        }
        let len = usize::try_from(end - start)
            .map_err(|_| io::Error::other("catalog pack record is too large"))?;
        let mut raw = vec![0_u8; len];
        self.reader.seek(SeekFrom::Start(start))?;
        self.reader.read_exact(&mut raw)?;
        self.remaining -= 1;
        Ok(Some((PackId::new(Digest::from(id)), raw)))
    }
}

struct ChunkIter {
    file: File,
    blocks: Vec<CatalogRunChunkBlockRef>,
    block_at: usize,
    reader: Option<BufReader<File>>,
    remaining: u64,
}

impl ChunkIter {
    fn new(path: &std::path::Path, query: &CatalogRunQueryIndex) -> io::Result<Self> {
        Ok(Self {
            file: File::open(path)?,
            blocks: query.chunks.clone(),
            block_at: 0,
            reader: None,
            remaining: 0,
        })
    }

    fn open_block(&mut self) -> io::Result<bool> {
        let Some(block) = self.blocks.get(self.block_at) else {
            return Ok(false);
        };
        self.block_at += 1;
        let mut reader = BufReader::new(self.file.try_clone()?);
        reader.seek(SeekFrom::Start(block.offset))?;
        let mut header = [0_u8; 16];
        reader.read_exact(&mut header)?;
        if header[..8] != *b"casircb1" {
            return Err(io::Error::other("invalid catalog run chunk block"));
        }
        let count = u64::from_le_bytes(header[8..].try_into().expect("eight bytes"));
        if 16_u64.checked_add(
            count
                .checked_mul(CHUNK_RECORD_BYTES)
                .ok_or_else(|| io::Error::other("catalog run chunk block count overflow"))?,
        ) != Some(block.encoded_bytes)
        {
            return Err(io::Error::other("catalog run chunk block length mismatch"));
        }
        self.reader = Some(reader);
        self.remaining = count;
        Ok(true)
    }

    fn next_entry(&mut self) -> io::Result<Option<IndexedLocation>> {
        if self.remaining == 0 && !self.open_block()? {
            return Ok(None);
        }
        let reader = self.reader.as_mut().expect("opened chunk block");
        let mut bytes = [0_u8; CHUNK_RECORD_BYTES as usize];
        reader.read_exact(&mut bytes)?;
        self.remaining -= 1;
        let digest =
            ChunkId::new(Digest::try_from(&bytes[..DIGEST_LEN]).map_err(io::Error::other)?);
        let pack = PackId::new(
            Digest::try_from(&bytes[DIGEST_LEN..DIGEST_LEN * 2]).map_err(io::Error::other)?,
        );
        let mut at = DIGEST_LEN * 2;
        let mut number = || {
            let value = u64::from_le_bytes(bytes[at..at + 8].try_into().expect("eight bytes"));
            at += 8;
            value
        };
        Ok(Some(IndexedLocation {
            digest,
            location: Location {
                pack,
                pack_len: number(),
                offset: number(),
                framed_len: number(),
                uncompressed_len: number(),
            },
        }))
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct ChunkHeapEntry {
    digest: ChunkId,
    pack: PackId,
    source: usize,
    entry: IndexedLocation,
}

impl Ord for ChunkHeapEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .digest
            .cmp(&self.digest)
            .then_with(|| other.pack.cmp(&self.pack))
            .then_with(|| other.source.cmp(&self.source))
    }
}

impl PartialOrd for ChunkHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn patch_u64(file: &mut File, offset: u64, value: u64) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&value.to_le_bytes())
}

fn copy_pack_category(
    output: &mut File,
    inputs: &[StagedCatalogRun],
    winners: &HashMap<PackId, usize>,
) -> io::Result<u64> {
    let mut iters = inputs
        .iter()
        .map(|run| PackIter::new(run.file.path(), run.layout.packs))
        .collect::<io::Result<Vec<_>>>()?;
    let mut current = iters
        .iter_mut()
        .map(PackIter::next_record)
        .collect::<io::Result<Vec<_>>>()?;
    let mut written = 0;
    while let Some(source) = current
        .iter()
        .enumerate()
        .filter_map(|(source, record)| record.as_ref().map(|(pack, _)| (source, *pack)))
        .min_by_key(|(_, pack)| *pack)
        .map(|(source, _)| source)
    {
        let (pack, raw) = current[source].take().expect("selected pack record");
        if winners.get(&pack) == Some(&source) {
            output.write_all(&raw)?;
            written += 1;
        }
        current[source] = iters[source].next_record()?;
    }
    Ok(written)
}

fn copy_pack_ids(
    output: &mut File,
    inputs: &[StagedCatalogRun],
    winners: &HashMap<PackId, usize>,
    section: impl Fn(&RunLayout) -> FixedSection,
) -> io::Result<u64> {
    let mut iters = inputs
        .iter()
        .map(|run| IdIter::new(run.file.path(), section(&run.layout)))
        .collect::<io::Result<Vec<_>>>()?;
    let mut current = iters
        .iter_mut()
        .map(|iter| iter.next_digest().map(|value| value.map(PackId::new)))
        .collect::<io::Result<Vec<_>>>()?;
    let mut written = 0;
    while let Some(source) = current
        .iter()
        .enumerate()
        .filter_map(|(source, pack)| pack.map(|pack| (source, pack)))
        .min_by_key(|(_, pack)| *pack)
        .map(|(source, _)| source)
    {
        let pack = current[source].take().expect("selected pack id");
        if winners.get(&pack) == Some(&source) {
            output.write_all(pack.as_digest().as_bytes())?;
            written += 1;
        }
        current[source] = iters[source].next_digest()?.map(PackId::new);
    }
    Ok(written)
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct ManifestEvent {
    id: BlobId,
    source: usize,
    added: bool,
    iterator: usize,
}

impl Ord for ManifestEvent {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .id
            .cmp(&self.id)
            .then_with(|| other.source.cmp(&self.source))
            .then_with(|| other.added.cmp(&self.added))
    }
}

impl PartialOrd for ManifestEvent {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn copy_manifests(
    output: &mut File,
    inputs: &[StagedCatalogRun],
    wanted_added: bool,
) -> io::Result<u64> {
    let mut iters = Vec::with_capacity(inputs.len() * 2);
    for run in inputs {
        iters.push((
            false,
            IdIter::new(run.file.path(), run.layout.removed_manifests)?,
        ));
        iters.push((true, IdIter::new(run.file.path(), run.layout.manifests)?));
    }
    let mut heap = BinaryHeap::new();
    for (iterator, (added, iter)) in iters.iter_mut().enumerate() {
        if let Some(id) = iter.next_digest()? {
            heap.push(ManifestEvent {
                id: BlobId::new(id),
                source: iterator / 2,
                added: *added,
                iterator,
            });
        }
    }
    let mut written = 0;
    while let Some(first) = heap.pop() {
        let id = first.id;
        let mut events = vec![first];
        while heap.peek().is_some_and(|event| event.id == id) {
            events.push(heap.pop().expect("peeked manifest event"));
        }
        let winner = events
            .iter()
            .max_by_key(|event| event.source)
            .copied()
            .expect("manifest event set is nonempty");
        if winner.added == wanted_added {
            output.write_all(id.as_digest().as_bytes())?;
            written += 1;
        }
        for event in events {
            let (added, iter) = &mut iters[event.iterator];
            if let Some(next) = iter.next_digest()? {
                heap.push(ManifestEvent {
                    id: BlobId::new(next),
                    source: event.source,
                    added: *added,
                    iterator: event.iterator,
                });
            }
        }
    }
    Ok(written)
}

fn flush_chunk_block(
    output: &mut File,
    entries: &mut Vec<IndexedLocation>,
    blocks: &mut Vec<CatalogRunChunkBlockRef>,
) -> io::Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let offset = output.stream_position()?;
    let bytes = encode_run_chunk_block(entries);
    let reference = CatalogRunChunkBlockRef {
        first: entries.first().expect("nonempty chunk block").digest,
        last: entries.last().expect("nonempty chunk block").digest,
        offset,
        encoded_bytes: bytes.len() as u64,
        digest: Digest::from(blake3::hash(&bytes)),
    };
    output.write_all(&bytes)?;
    blocks.push(reference);
    entries.clear();
    Ok(())
}

pub(super) fn merge_staged_runs(inputs: &[StagedCatalogRun]) -> io::Result<PreparedCatalogRunFile> {
    let first = inputs
        .first()
        .ok_or_else(|| io::Error::other("cannot merge an empty catalog run set"))?;
    for pair in inputs.windows(2) {
        if pair[0].reference.last_generation.checked_add(1)
            != Some(pair[1].reference.first_generation)
        {
            return Err(io::Error::other(
                "catalog run generations are not contiguous",
            ));
        }
    }
    let first_generation = first.reference.first_generation;
    let last_generation = inputs
        .last()
        .expect("nonempty inputs")
        .reference
        .last_generation;
    let mut winners = HashMap::new();
    for (source, run) in inputs.iter().enumerate() {
        for pack in &run.query.changed_packs {
            winners.insert(*pack, source);
        }
    }

    let mut temporary = NamedTempFile::new()?;
    let output = temporary.as_file_mut();
    output.write_all(&CATALOG_RUN_MAGIC_V2)?;
    output.write_all(&first_generation.to_le_bytes())?;
    output.write_all(&last_generation.to_le_bytes())?;
    output.write_all(&[0_u8; DIGEST_LEN])?;
    output.write_all(&0_u64.to_le_bytes())?;
    let delta_start = output.stream_position()?;
    output.write_all(&DELTA_MAGIC)?;

    let removed_count_at = output.stream_position()?;
    output.write_all(&0_u64.to_le_bytes())?;
    let removed = copy_pack_ids(output, inputs, &winners, |layout| layout.removed_packs)?;

    let removed_manifests_at = output.stream_position()?;
    output.write_all(&0_u64.to_le_bytes())?;
    let removed_manifests = copy_manifests(output, inputs, false)?;

    let patch_len_at = output.stream_position()?;
    output.write_all(&0_u64.to_le_bytes())?;
    let patch_start = output.stream_position()?;
    output.write_all(&CHECKPOINT_MAGIC)?;
    output.write_all(blake3::hash(DELTA_INVENTORY).as_bytes())?;
    let pack_count_at = output.stream_position()?;
    output.write_all(&0_u64.to_le_bytes())?;
    let packs = copy_pack_category(output, inputs, &winners)?;

    let superseded_count_at = output.stream_position()?;
    output.write_all(&0_u64.to_le_bytes())?;
    let superseded = copy_pack_ids(output, inputs, &winners, |layout| layout.superseded)?;
    output.write_all(&[u8::from(
        inputs.iter().any(|run| run.layout.manifests_complete),
    )])?;
    let manifest_count_at = output.stream_position()?;
    output.write_all(&0_u64.to_le_bytes())?;
    let manifests = copy_manifests(output, inputs, true)?;
    let patch_end = output.stream_position()?;
    let delta_end = patch_end;

    patch_u64(output, removed_count_at, removed)?;
    patch_u64(output, removed_manifests_at, removed_manifests)?;
    patch_u64(output, patch_len_at, patch_end - patch_start)?;
    patch_u64(output, pack_count_at, packs)?;
    patch_u64(output, superseded_count_at, superseded)?;
    patch_u64(output, manifest_count_at, manifests)?;
    patch_u64(output, 56, delta_end - delta_start)?;

    let mut checksum = blake3::Hasher::new();
    checksum.update(CATALOG_RUN_DOMAIN);
    checksum.update(&first_generation.to_le_bytes());
    checksum.update(&last_generation.to_le_bytes());
    checksum.update(&(delta_end - delta_start).to_le_bytes());
    output.seek(SeekFrom::Start(delta_start))?;
    let mut remaining = delta_end - delta_start;
    let mut buffer = vec![0_u8; 1024 * 1024];
    while remaining != 0 {
        let take = usize::try_from(remaining.min(buffer.len() as u64)).expect("bounded read");
        output.read_exact(&mut buffer[..take])?;
        checksum.update(&buffer[..take]);
        remaining -= take as u64;
    }
    output.seek(SeekFrom::Start(24))?;
    output.write_all(checksum.finalize().as_bytes())?;

    output.seek(SeekFrom::Start(delta_end))?;
    let mut iters = inputs
        .iter()
        .map(|run| ChunkIter::new(run.file.path(), &run.query))
        .collect::<io::Result<Vec<_>>>()?;
    let mut heap = BinaryHeap::new();
    for (source, iter) in iters.iter_mut().enumerate() {
        if let Some(entry) = iter.next_entry()? {
            heap.push(ChunkHeapEntry {
                digest: entry.digest,
                pack: entry.location.pack,
                source,
                entry,
            });
        }
    }
    let mut block_entries = Vec::with_capacity(1024);
    let mut blocks = Vec::new();
    let mut previous = None;
    while let Some(event) = heap.pop() {
        if winners.get(&event.pack) == Some(&event.source)
            && previous != Some((event.digest, event.pack))
        {
            if block_entries.len() >= 1024
                && block_entries
                    .last()
                    .is_some_and(|entry: &IndexedLocation| entry.digest != event.digest)
            {
                flush_chunk_block(output, &mut block_entries, &mut blocks)?;
            }
            block_entries.push(event.entry);
            previous = Some((event.digest, event.pack));
        }
        if let Some(entry) = iters[event.source].next_entry()? {
            heap.push(ChunkHeapEntry {
                digest: entry.digest,
                pack: entry.location.pack,
                source: event.source,
                entry,
            });
        }
    }
    flush_chunk_block(output, &mut block_entries, &mut blocks)?;
    let directory_offset = output.stream_position()?;
    let mut changed_packs = winners.keys().copied().collect::<Vec<_>>();
    changed_packs.sort_unstable();
    let tail = encode_run_query_tail(
        &CatalogRunQueryIndex {
            chunks: blocks,
            changed_packs,
        },
        directory_offset,
    )?;
    output.write_all(&tail)?;
    output.flush()?;
    let object_len = output.metadata()?.len();
    let mut object_hash = blake3::Hasher::new();
    output.seek(SeekFrom::Start(0))?;
    loop {
        let read = output.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        object_hash.update(&buffer[..read]);
    }
    let digest = Digest::from(object_hash.finalize());
    let routing_len = tail.len() - RUN_QUERY_TRAILER_BYTES;
    let query = CatalogRunQueryRef {
        offset: directory_offset,
        encoded_bytes: tail.len() as u64,
        digest: Digest::from(blake3::hash(&tail)),
        routing: Bytes::copy_from_slice(&tail[..routing_len]),
    };
    Ok(PreparedCatalogRunFile {
        file: temporary,
        reference: CatalogRunRef {
            digest,
            first_generation,
            last_generation,
            encoded_bytes: object_len,
            query: Some(query),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::super::delta::{apply_index_delta, decode_index_delta, encode_index_delta};
    use super::super::run::{
        CatalogRun, catalog_run_query_ref, decode_catalog_run, decode_catalog_run_routing,
    };
    use super::*;

    fn entry(label: &[u8], offset: u64) -> PackEntry {
        PackEntry {
            digest: ChunkId::new(blake3::hash(label).into()),
            offset,
            framed_len: 32,
            uncompressed_len: 64,
        }
    }

    fn stage(run: CatalogRun) -> StagedCatalogRun {
        let encoded = encode_catalog_run(&run).unwrap();
        let reference = CatalogRunRef {
            digest: Digest::from(blake3::hash(&encoded)),
            first_generation: run.first_generation,
            last_generation: run.last_generation,
            encoded_bytes: encoded.len() as u64,
            query: Some(catalog_run_query_ref(&encoded).unwrap()),
        };
        stage_bytes(&encoded, reference).unwrap()
    }

    fn assert_same(left: &Index, right: &Index) {
        let inventory = Digest::from(blake3::hash(b"streamed catalog merge equality"));
        assert_eq!(
            encode_index_checkpoint(left, inventory).unwrap(),
            encode_index_checkpoint(right, inventory).unwrap()
        );
    }

    #[test]
    fn streamed_merge_preserves_exact_last_writer_wins_state() {
        let retained_pack = PackId::new(blake3::hash(b"stream retained pack").into());
        let removed_pack = PackId::new(blake3::hash(b"stream removed pack").into());
        let added_pack = PackId::new(blake3::hash(b"stream added pack").into());
        let restored_manifest = BlobId::new(blake3::hash(b"stream restored manifest").into());
        let removed_manifest = BlobId::new(blake3::hash(b"stream removed manifest").into());
        let retained_entries = vec![
            entry(b"stream retained live chunk", 0),
            entry(b"stream tombstoned chunk", 32),
        ];

        let mut base = Index::default();
        base.add_pack(retained_pack, 256, retained_entries.clone());
        base.add_pack(removed_pack, 128, vec![entry(b"stream removed chunk", 0)]);
        base.manifests.extend([restored_manifest, removed_manifest]);
        base.manifests_complete = true;
        base.rebuild_chunks();

        let mut middle = base.clone();
        middle.remove_pack(removed_pack);
        middle
            .tombstoned
            .insert(retained_pack, HashSet::from([retained_entries[1].digest]));
        middle.manifests.remove(&restored_manifest);
        middle.manifests.remove(&removed_manifest);
        middle.rebuild_chunks();

        let mut final_index = middle.clone();
        final_index.add_pack(added_pack, 128, vec![entry(b"stream added chunk", 0)]);
        final_index.manifests.insert(restored_manifest);
        final_index.rebuild_chunks();

        let staged = vec![
            stage(CatalogRun {
                first_generation: 2,
                last_generation: 2,
                delta: encode_index_delta(&base, &middle).unwrap(),
            }),
            stage(CatalogRun {
                first_generation: 3,
                last_generation: 3,
                delta: encode_index_delta(&middle, &final_index).unwrap(),
            }),
        ];
        let merged = merge_staged_runs(&staged).unwrap();
        let encoded = std::fs::read(merged.file.path()).unwrap();
        assert_eq!(encoded.len() as u64, merged.reference.encoded_bytes);
        assert_eq!(
            Digest::from(blake3::hash(&encoded)),
            merged.reference.digest
        );
        assert_eq!(
            catalog_run_query_ref(&encoded).unwrap(),
            merged.reference.query.unwrap()
        );
        let merged = decode_catalog_run(&encoded).unwrap();
        let decoded = decode_index_delta(&merged.delta).unwrap();
        assert_eq!(decoded.removed_manifests, HashSet::from([removed_manifest]));
        assert_eq!(
            decoded.patch.manifests.sorted_ids(),
            vec![restored_manifest]
        );
        let mut reconstructed = base;
        apply_index_delta(&mut reconstructed, &merged.delta).unwrap();
        assert_same(&reconstructed, &final_index);
    }

    #[test]
    fn streamed_merge_retains_duplicate_live_chunk_locations() {
        let chunk = entry(b"stream duplicate chunk", 0);
        let first_pack = PackId::new(blake3::hash(b"stream duplicate first pack").into());
        let second_pack = PackId::new(blake3::hash(b"stream duplicate second pack").into());
        let mut first = Index::default();
        first.add_pack(first_pack, 128, vec![chunk]);
        first.rebuild_chunks();
        let mut second = first.clone();
        second.add_pack(second_pack, 256, vec![chunk]);
        second.rebuild_chunks();
        let staged = vec![
            stage(CatalogRun {
                first_generation: 1,
                last_generation: 1,
                delta: encode_index_delta(&Index::default(), &first).unwrap(),
            }),
            stage(CatalogRun {
                first_generation: 2,
                last_generation: 2,
                delta: encode_index_delta(&first, &second).unwrap(),
            }),
        ];

        let merged = merge_staged_runs(&staged).unwrap();
        let encoded = std::fs::read(merged.file.path()).unwrap();
        let query = decode_catalog_run_routing(merged.reference.query.as_ref().unwrap()).unwrap();
        let block = query.chunk_block(&chunk.digest).unwrap();
        let start = usize::try_from(block.offset).unwrap();
        let count = u64::from_le_bytes(encoded[start + 8..start + 16].try_into().unwrap());
        assert_eq!(count, 2);

        let run = decode_catalog_run(&encoded).unwrap();
        let mut reconstructed = Index::default();
        apply_index_delta(&mut reconstructed, &run.delta).unwrap();
        assert_same(&reconstructed, &second);
    }

    #[test]
    fn streamed_merge_rejects_generation_gaps() {
        let empty = Index::default();
        let staged = vec![
            stage(CatalogRun {
                first_generation: 1,
                last_generation: 1,
                delta: encode_index_delta(&empty, &empty).unwrap(),
            }),
            stage(CatalogRun {
                first_generation: 3,
                last_generation: 3,
                delta: encode_index_delta(&empty, &empty).unwrap(),
            }),
        ];
        assert!(merge_staged_runs(&staged).is_err());
    }

    #[test]
    fn staging_rejects_delta_and_query_corruption_independently() {
        let empty = Index::default();
        let run = CatalogRun {
            first_generation: 1,
            last_generation: 1,
            delta: encode_index_delta(&empty, &empty).unwrap(),
        };
        let encoded = encode_catalog_run(&run).unwrap();
        let reference = CatalogRunRef {
            digest: Digest::from(blake3::hash(&encoded)),
            first_generation: 1,
            last_generation: 1,
            encoded_bytes: encoded.len() as u64,
            query: Some(catalog_run_query_ref(&encoded).unwrap()),
        };

        let mut bad_delta = encoded.to_vec();
        bad_delta[RUN_HEADER_BYTES as usize + 8] ^= 1;
        let mut bad_delta_reference = reference.clone();
        bad_delta_reference.digest = Digest::from(blake3::hash(&bad_delta));
        assert!(stage_bytes(&bad_delta, bad_delta_reference).is_err());

        let mut bad_query = encoded.to_vec();
        let query_at = usize::try_from(reference.query.as_ref().unwrap().offset).unwrap();
        bad_query[query_at] ^= 1;
        let mut bad_query_reference = reference;
        bad_query_reference.digest = Digest::from(blake3::hash(&bad_query));
        assert!(stage_bytes(&bad_query, bad_query_reference).is_err());
    }
}
