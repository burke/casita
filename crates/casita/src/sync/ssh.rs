//! Authenticated machine-to-machine transfer sources over SSH stdio.
//!
//! SSH supplies peer authentication, host-key verification, encryption, and
//! operator policy. This module supplies only a bounded versioned RPC adapter
//! for [`TransferReadSession`]. The destination
//! still performs traversal, namespace verification, mutation, and root
//! installation through the ordinary transfer engine.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use data_encoding::BASE64URL_NOPAD;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::process::{Child, Command};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::blob::BlobStore;
use crate::directory::Directory;
use crate::metadata::MetadataStore;
use crate::node::Node;
use crate::object::{ObjectKey, ObjectRecord, RepositoryRevision, RootName};
use crate::path::PathComponent;
use crate::repository::RetentionHold;
use crate::sync::TransferBatchPayload;
use crate::sync::TransferDiscoveryAnswer;
use crate::sync::sliced::{
    BlobSliceSources, NoSources, SliceError, SliceIndex, SliceSources, decode_sliced,
    encode_literal_stream, encode_sliced_stream,
};
use crate::sync::{
    DISCOVERY_ANSWER_PAYLOAD_BYTES, DISCOVERY_ANSWER_PAYLOAD_OBJECTS, MAX_PAYLOAD_BATCH_BYTES,
    MAX_PAYLOAD_BATCH_OBJECTS, MAX_TRANSFER_REQUEST_BYTES, MAX_TRANSFER_REQUEST_OBJECTS, PathProof,
    PathProofEntry, PathProofResponse, SliceBasePair, SliceBases, SlicedReceipt, TransferError,
    TransferPayloadReader, TransferReadSession, TransferSelection, TransferSource,
};
use crate::{RepositoryError, repository::Repository};

// Client and server ship together: the protocol has one version, and any other
// magic is a mismatch rather than a subset to negotiate.
const PROTOCOL_MAGIC: &[u8] = b"casita-ssh-source-v3\0";
const REQUEST_HEADER_LEN: usize = 9;
const RESPONSE_HEADER_LEN: usize = 9;
const MAX_CONTROL_FRAME: usize = MAX_TRANSFER_REQUEST_BYTES;
const MAX_ERROR_FRAME: usize = 64 * 1024;

const OP_OBJECT: u8 = 1;
const OP_ROOT: u8 = 2;
const OP_PAYLOAD: u8 = 3;
const OP_OBJECTS: u8 = 4;
const OP_PATH_PROOF: u8 = 5;
const OP_PAYLOADS: u8 = 6;
const OP_PROOF: u8 = 7;
const OP_SELECTION: u8 = 8;
const OP_SLICE_BASES: u8 = 9;
const OP_SLICED: u8 = 10;
const OP_DISCOVER: u8 = 11;
/// Objects a discovery request may name before its answer stops volunteering
/// payloads. A receiver naming one or two objects is syncing a tree, and the
/// answer is all the work it has: payloads riding along save it round trips
/// nothing else would hide. A receiver naming a frontier of hundreds is
/// walking a closure with a payload batch pipeline behind it, and that
/// pipeline checks what the destination already holds before asking, which
/// is the one fact this side does not have.
const VOLUNTEER_MAX_REQUEST_KEYS: usize = 8;
/// Sampled discovery chunks one serving process indexes for slicing: about
/// 10 GiB of held payloads at the default sampling, roughly 160 MiB of memory.
const SLICE_INDEX_MAX_ENTRIES: usize = 1 << 21;
/// Objects one serving process walks while indexing offered closures.
const SLICE_BASE_MAX_OBJECTS: usize = 1 << 20;

const STATUS_OK: u8 = 0;
const STATUS_ABSENT: u8 = 1;
const STATUS_ERROR: u8 = 2;
const STATUS_FALLBACK: u8 = 3;

/// A validated `ssh://[user@]host[:port]/absolute/repository/path` endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SshEndpoint {
    destination: String,
    port: Option<u16>,
    repository: PathBuf,
}

impl SshEndpoint {
    /// OpenSSH destination (`host` or `user@host`) after strict validation.
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Explicit SSH port, if supplied.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Absolute repository path passed to the remote Casita process.
    pub fn repository(&self) -> &Path {
        &self.repository
    }

    fn encoded_repository(&self) -> String {
        BASE64URL_NOPAD.encode(
            self.repository
                .to_str()
                .expect("SSH endpoint paths are validated UTF-8")
                .as_bytes(),
        )
    }
}

impl fmt::Display for SshEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ssh://{}", self.destination)?;
        if let Some(port) = self.port {
            write!(formatter, ":{port}")?;
        }
        write!(formatter, "{}", self.repository.display())
    }
}

impl FromStr for SshEndpoint {
    type Err = SshEndpointError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let remainder = value
            .strip_prefix("ssh://")
            .ok_or(SshEndpointError::Scheme)?;
        let (authority, path) = remainder
            .split_once('/')
            .ok_or(SshEndpointError::RepositoryPath)?;
        if authority.is_empty() || authority.len() > 512 {
            return Err(SshEndpointError::Authority);
        }
        if path.contains(['?', '#']) {
            return Err(SshEndpointError::RepositoryPath);
        }

        let (user, host_port) = match authority.rsplit_once('@') {
            Some((user, host)) => {
                validate_component(user, true)?;
                if user.contains('@') {
                    return Err(SshEndpointError::Authority);
                }
                (Some(user), host)
            }
            None => (None, authority),
        };

        let (host, port) = parse_host_port(host_port)?;
        let destination = match user {
            Some(user) => format!("{user}@{host}"),
            None => host,
        };

        let decoded = percent_decode(&format!("/{path}"))?;
        if decoded.len() > 16 * 1024 || decoded.contains(&0) {
            return Err(SshEndpointError::RepositoryPath);
        }
        let decoded = String::from_utf8(decoded).map_err(|_| SshEndpointError::RepositoryPath)?;
        // This is a remote URL path. A Windows client must accept a Unix
        // server's /absolute/path without applying local drive-letter rules.
        if !decoded.starts_with('/') {
            return Err(SshEndpointError::RepositoryPath);
        }
        let repository = PathBuf::from(decoded);

        Ok(Self {
            destination,
            port,
            repository,
        })
    }
}

/// Invalid SSH transfer-source endpoint.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SshEndpointError {
    /// Only the explicit SSH scheme is accepted.
    #[error("SSH source must start with `ssh://`")]
    Scheme,
    /// User, host, or port is missing, malformed, or unsafe for OpenSSH.
    #[error("invalid SSH endpoint authority")]
    Authority,
    /// Port is not a nonzero 16-bit integer.
    #[error("invalid SSH endpoint port")]
    Port,
    /// Repository path is missing, non-UTF-8, relative, or malformed.
    #[error("SSH repository path must be one absolute UTF-8 URL path")]
    RepositoryPath,
    /// Percent escape in the repository path is malformed.
    #[error("invalid percent escape in SSH repository path")]
    PercentEscape,
}

fn validate_component(value: &str, user: bool) -> Result<(), SshEndpointError> {
    let valid = !value.is_empty()
        && !value.starts_with('-')
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b'-')
                || (!user && byte == b'%')
        });
    if valid {
        Ok(())
    } else {
        Err(SshEndpointError::Authority)
    }
}

fn parse_host_port(value: &str) -> Result<(String, Option<u16>), SshEndpointError> {
    if let Some(bracketed) = value.strip_prefix('[') {
        let end = bracketed.find(']').ok_or(SshEndpointError::Authority)?;
        let host = &bracketed[..end];
        if host.is_empty()
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() || matches!(byte, b':' | b'.'))
        {
            return Err(SshEndpointError::Authority);
        }
        let suffix = &bracketed[end + 1..];
        let port = if suffix.is_empty() {
            None
        } else {
            Some(parse_port(
                suffix
                    .strip_prefix(':')
                    .ok_or(SshEndpointError::Authority)?,
            )?)
        };
        return Ok((format!("[{host}]"), port));
    }

    if value.matches(':').count() > 1 {
        return Err(SshEndpointError::Authority);
    }
    let (host, port) = match value.rsplit_once(':') {
        Some((host, port)) => (host, Some(parse_port(port)?)),
        None => (value, None),
    };
    validate_component(host, false)?;
    Ok((host.to_owned(), port))
}

fn parse_port(value: &str) -> Result<u16, SshEndpointError> {
    let port = value.parse::<u16>().map_err(|_| SshEndpointError::Port)?;
    if port == 0 {
        Err(SshEndpointError::Port)
    } else {
        Ok(port)
    }
}

fn percent_decode(value: &str) -> Result<Vec<u8>, SshEndpointError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut cursor = 0usize;
    while cursor < bytes.len() {
        if bytes[cursor] == b'%' {
            let pair = bytes
                .get(cursor + 1..cursor + 3)
                .ok_or(SshEndpointError::PercentEscape)?;
            let high = hex_nibble(pair[0]).ok_or(SshEndpointError::PercentEscape)?;
            let low = hex_nibble(pair[1]).ok_or(SshEndpointError::PercentEscape)?;
            decoded.push((high << 4) | low);
            cursor += 3;
        } else {
            decoded.push(bytes[cursor]);
            cursor += 1;
        }
    }
    Ok(decoded)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// A [`TransferSource`] reached by spawning OpenSSH and a hidden remote
/// Casita stdio source process.
///
/// Host authentication, user authentication, encryption, proxy jumps, agent
/// use, and known-host policy are all inherited from the user's OpenSSH
/// configuration. Casita never weakens host-key checking.
///
/// Casita also bounds liveness, so a peer that stops answering fails the
/// transfer instead of blocking it forever: connection setup must finish
/// within 30 seconds, and keepalives over the encrypted channel detect a host
/// or network that went silent within about 45 seconds. These command-line
/// options take precedence over the same settings in `ssh_config`.
#[derive(Debug, Clone)]
pub struct SshTransferSource {
    endpoint: SshEndpoint,
    program: OsString,
}

impl SshTransferSource {
    /// Use the system `ssh` program for this endpoint.
    pub fn new(endpoint: SshEndpoint) -> Self {
        Self {
            endpoint,
            program: OsString::from("ssh"),
        }
    }

    /// Use an explicit OpenSSH-compatible client executable.
    pub fn with_program(endpoint: SshEndpoint, program: impl Into<OsString>) -> Self {
        Self {
            endpoint,
            program: program.into(),
        }
    }

    /// Validated remote endpoint.
    pub fn endpoint(&self) -> &SshEndpoint {
        &self.endpoint
    }

    fn arguments(&self) -> Vec<OsString> {
        let mut arguments = [
            "-T",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ConnectTimeout=30",
            "-o",
            "ServerAliveInterval=15",
            "-o",
            "ServerAliveCountMax=3",
        ]
        .map(OsString::from)
        .to_vec();
        if let Some(port) = self.endpoint.port {
            arguments.push(OsString::from("-p"));
            arguments.push(OsString::from(port.to_string()));
        }
        arguments.extend([
            OsString::from(&self.endpoint.destination),
            OsString::from("casita"),
            OsString::from("__ssh-source"),
            OsString::from("--repository-base64"),
            OsString::from(self.endpoint.encoded_repository()),
        ]);
        arguments
    }

    #[tracing::instrument(name = "ssh.connect", skip_all)]
    async fn connect(&self) -> Result<SshTransferSession, TransferError> {
        let mut command = Command::new(&self.program);
        command
            .args(self.arguments())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            TransferError::SourceTransport(format!(
                "could not start {}: {error}",
                self.program.to_string_lossy()
            ))
        })?;
        let writer = child.stdin.take().ok_or_else(|| {
            TransferError::SourceTransport("SSH child has no stdin pipe".to_owned())
        })?;
        let reader = child.stdout.take().ok_or_else(|| {
            TransferError::SourceTransport("SSH child has no stdout pipe".to_owned())
        })?;
        SshTransferSession::connect(Box::new(reader), Box::new(writer), Some(child)).await
    }
}

#[async_trait]
impl TransferSource for SshTransferSource {
    async fn begin_transfer(
        &self,
        selection: TransferSelection,
    ) -> Result<Box<dyn TransferReadSession + '_>, TransferError> {
        selection.validate()?;
        let session = self.connect().await?;
        session.select(&selection).await?;
        Ok(Box::new(session))
    }
}

struct SshTransferSession {
    revision: RepositoryRevision,
    connection: Arc<RpcConnection>,
}

/// Human names for the request operations, for transport breakdowns.
fn operation_name(operation: u8) -> &'static str {
    match operation {
        OP_OBJECT => "object",
        OP_ROOT => "root",
        OP_PAYLOAD => "payload",
        OP_OBJECTS => "objects",
        OP_PATH_PROOF => "path-proof",
        OP_PAYLOADS => "payloads",
        OP_PROOF => "proof",
        OP_SELECTION => "selection",
        OP_SLICE_BASES => "slice-bases",
        OP_SLICED => "sliced",
        OP_DISCOVER => "discover",
        _ => "unknown",
    }
}

impl SshTransferSession {
    async fn select(&self, selection: &TransferSelection) -> Result<(), TransferError> {
        selection.validate()?;
        if matches!(selection, TransferSelection::Snapshot) {
            return Ok(());
        }
        let encoded = encode_selection(selection)?;
        if self.control_request(OP_SELECTION, &encoded).await? != Some(Vec::new()) {
            return Err(TransferError::SourceTransport(
                "source did not acknowledge transfer selection".into(),
            ));
        }
        Ok(())
    }

    #[tracing::instrument(name = "ssh.handshake", skip_all)]
    async fn connect(
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
        child: Option<Child>,
    ) -> Result<Self, TransferError> {
        let mut magic = vec![0u8; PROTOCOL_MAGIC.len()];
        reader.read_exact(&mut magic).await.map_err(transport_io)?;
        if magic != PROTOCOL_MAGIC {
            return Err(TransferError::SourceTransport(
                "remote source protocol/version mismatch".to_owned(),
            ));
        }
        let mut revision = [0u8; 32];
        reader
            .read_exact(&mut revision)
            .await
            .map_err(transport_io)?;
        let revision = RepositoryRevision::from_bytes(revision);
        tracing::info!(revision = %revision, "SSH transfer session established");
        Ok(Self {
            revision,
            connection: Arc::new(RpcConnection::new(reader, writer, child)),
        })
    }

    #[tracing::instrument(
        name = "ssh.control_request",
        level = "debug",
        skip_all,
        fields(operation = operation, request_bytes = body.len())
    )]
    async fn control_request(
        &self,
        operation: u8,
        body: &[u8],
    ) -> Result<Option<Vec<u8>>, TransferError> {
        let ticket = self.connection.send(operation, body).await?;
        let mut turn = self.connection.receive(ticket).await?;
        let (status, length) = read_response_header(turn.reader()).await?;
        let response = match status {
            STATUS_OK => {
                let length = bounded_length(length, MAX_CONTROL_FRAME, "control response")?;
                let mut response = vec![0u8; length];
                turn.reader()
                    .read_exact(&mut response)
                    .await
                    .map_err(transport_io)?;
                Some(response)
            }
            STATUS_ABSENT if length == 0 => None,
            STATUS_ERROR => {
                let error = read_remote_error(turn.reader(), length).await;
                turn.finish();
                return Err(error);
            }
            _ => {
                return Err(TransferError::SourceTransport(
                    "invalid remote source response status".to_owned(),
                ));
            }
        };
        turn.finish();
        Ok(response)
    }
}

/// Connect to one bounded stdio transfer source over caller-supplied I/O.
///
/// This function supplies no authentication, encryption, or peer identity.
/// Callers must provide an already authenticated and confidential channel,
/// such as OpenSSH. It is also useful for composing the protocol with a
/// controlled in-process transport in benchmarks and conformance tests.
#[tracing::instrument(name = "ssh.connect_stdio_source", skip_all)]
pub async fn connect_transfer_stdio_source<R, W>(
    reader: R,
    writer: W,
    selection: TransferSelection,
) -> Result<impl TransferReadSession, TransferError>
where
    R: AsyncRead + Send + Unpin + 'static,
    W: AsyncWrite + Send + Unpin + 'static,
{
    selection.validate()?;
    let session = SshTransferSession::connect(Box::new(reader), Box::new(writer), None).await?;
    session.select(&selection).await?;
    Ok(session)
}

#[async_trait]
impl TransferReadSession for SshTransferSession {
    fn revision(&self) -> RepositoryRevision {
        self.revision
    }

    async fn object(&self, key: &ObjectKey) -> Result<Option<ObjectRecord>, TransferError> {
        let Some(encoded) = self.control_request(OP_OBJECT, &key.encode()).await? else {
            return Ok(None);
        };
        let record = ObjectRecord::decode(&encoded)
            .map_err(|error| TransferError::SourceTransport(error.to_string()))?;
        if record.key() != key {
            return Err(TransferError::SourceTransport(format!(
                "remote returned record {} for requested {key}",
                record.key()
            )));
        }
        Ok(Some(record))
    }

    async fn objects(
        &self,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRecord>>, TransferError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let request = encode_object_keys(keys)?;
        let encoded = self
            .control_request(OP_OBJECTS, &request)
            .await?
            .ok_or_else(|| {
                TransferError::SourceTransport(
                    "remote returned absent for an object batch".to_owned(),
                )
            })?;
        decode_requested_records(keys, &encoded)
    }

    async fn root(&self, name: &RootName) -> Result<Option<ObjectKey>, TransferError> {
        let Some(encoded) = self
            .control_request(OP_ROOT, name.as_str().as_bytes())
            .await?
        else {
            return Ok(None);
        };
        ObjectKey::decode(&encoded)
            .map(Some)
            .map_err(|error| TransferError::SourceTransport(error.to_string()))
    }

    async fn path_proof(
        &self,
        root: &RootName,
        components: &[PathComponent],
    ) -> Result<PathProofResponse, TransferError> {
        let request = crate::sync::wire::encode_path_proof_request(root, components);
        if request.len() > MAX_CONTROL_FRAME {
            return Ok(PathProofResponse::Unsupported);
        }

        let ticket = self.connection.send(OP_PATH_PROOF, &request).await?;
        let mut turn = self.connection.receive(ticket).await?;
        let (status, length) = read_response_header(turn.reader()).await?;
        let response = match status {
            STATUS_OK => {
                let length = bounded_length(length, MAX_CONTROL_FRAME, "path proof")?;
                let mut encoded = vec![0u8; length];
                turn.reader()
                    .read_exact(&mut encoded)
                    .await
                    .map_err(transport_io)?;
                let proof = crate::sync::wire::decode_path_proof(
                    &encoded,
                    components.len().saturating_add(1),
                    MAX_CONTROL_FRAME,
                )
                .map_err(|error| TransferError::SourceTransport(error.to_string()))?;
                PathProofResponse::Proof(proof)
            }
            STATUS_ABSENT if length == 0 => PathProofResponse::MissingRoot,
            STATUS_FALLBACK if length == 0 => PathProofResponse::Unsupported,
            STATUS_ERROR => {
                let error = read_remote_error(turn.reader(), length).await;
                turn.finish();
                return Err(error);
            }
            _ => {
                return Err(TransferError::SourceTransport(
                    "invalid remote path-proof response status".to_owned(),
                ));
            }
        };
        turn.finish();
        Ok(response)
    }

    fn transport_requests(&self) -> Option<u64> {
        Some(self.connection.requests.load(Ordering::Relaxed))
    }

    fn transport_operations(&self) -> Option<Vec<(String, u64)>> {
        let operations = self
            .connection
            .operations
            .lock()
            .expect("operation counters");
        Some(
            operations
                .iter()
                .map(|(operation, count)| (operation_name(*operation).to_owned(), *count))
                .collect(),
        )
    }

    fn supports_payload_batch(&self) -> bool {
        true
    }

    fn supports_object_payload_batch(&self) -> bool {
        true
    }

    async fn object_payload_batch(
        &self,
        keys: &[ObjectKey],
        max_bytes: usize,
        sources: &dyn SliceSources,
    ) -> Result<Option<crate::sync::TransferObjectBatch>, TransferError> {
        // The existing payload command has a fixed 1 MiB server limit. Smaller
        // caller budgets use the metadata-first path instead of speculating.
        if !self.supports_object_payload_batch()
            || max_bytes < MAX_PAYLOAD_BATCH_BYTES
            || keys.len() > MAX_PAYLOAD_BATCH_OBJECTS
            || keys.is_empty()
        {
            return Ok(None);
        }
        let request = encode_object_keys(keys)?;
        // Both commands go out before either answer is read, but the answers
        // are consumed while the second command may still be blocked on a
        // bounded transport, so the sends and the receives run together.
        let (tell, told) = tokio::sync::oneshot::channel();
        let requests = [(OP_OBJECTS, request.clone()), (OP_PAYLOADS, request)];
        let send = self.connection.send_pipelined(&requests, tell);
        let receive = async {
            let first = told.await.map_err(|_| {
                TransferError::SourceTransport("pipelined request was not sent".into())
            })?;
            let mut turn = self.connection.receive(first).await?;
            let (status, length) = read_response_header(turn.reader()).await?;
            if status != STATUS_OK {
                return Err(TransferError::SourceTransport(
                    "invalid pipelined metadata response".into(),
                ));
            }
            let length = bounded_length(length, MAX_CONTROL_FRAME, "pipelined metadata")?;
            let mut encoded = vec![0; length];
            turn.reader()
                .read_exact(&mut encoded)
                .await
                .map_err(transport_io)?;
            let records = decode_requested_records(keys, &encoded)?;
            turn.finish();

            let mut turn = self.connection.receive(first + 1).await?;
            let (status, count) = read_response_header(turn.reader()).await?;
            if status == STATUS_FALLBACK && count == 0 {
                turn.finish();
                return Ok(crate::sync::TransferObjectBatch {
                    records,
                    payloads: None,
                });
            }
            if status != STATUS_OK || count != keys.len() as u64 {
                return Err(TransferError::SourceTransport(
                    "invalid pipelined payload count".into(),
                ));
            }
            let mut payloads = Vec::with_capacity(keys.len());
            let mut remaining = MAX_PAYLOAD_BATCH_BYTES as u64;
            for record in &records {
                let (status, length) = read_response_header(turn.reader()).await?;
                let Some(record) = record.as_ref().filter(|record| {
                    status == STATUS_OK && length <= remaining && record.payload_size() == length
                }) else {
                    return Err(TransferError::SourceTransport(
                        "invalid pipelined payload length".into(),
                    ));
                };
                remaining -= length;
                payloads.push(receive_batched_frame(turn.boxed(), record, sources).await?);
            }
            turn.finish();
            Ok(crate::sync::TransferObjectBatch {
                records,
                payloads: Some(payloads),
            })
        };
        let (_, batch) = tokio::try_join!(send, receive)?;
        Ok(Some(batch))
    }

    fn supports_discovery(&self) -> bool {
        true
    }

    async fn discover(
        &self,
        keys: &[ObjectKey],
        sources: &dyn SliceSources,
    ) -> Result<Option<TransferDiscoveryAnswer>, TransferError> {
        if !self.supports_discovery()
            || keys.is_empty()
            || keys.len() > MAX_TRANSFER_REQUEST_OBJECTS
        {
            return Ok(None);
        }
        let request = encode_object_keys(keys)?;
        let ticket = self.connection.send(OP_DISCOVER, &request).await?;
        let mut turn = self.connection.receive(ticket).await?;
        // Three parts follow: the records, the keys whose payloads follow,
        // then one sliced frame per such key.
        let (status, length) = read_response_header(turn.reader()).await?;
        match status {
            STATUS_OK => {}
            STATUS_FALLBACK if length == 0 => {
                turn.finish();
                return Ok(None);
            }
            STATUS_ERROR => {
                let error = read_remote_error(turn.reader(), length).await;
                turn.finish();
                return Err(error);
            }
            _ => {
                return Err(TransferError::SourceTransport(
                    "invalid discovery response status".into(),
                ));
            }
        }
        let length = bounded_length(length, MAX_CONTROL_FRAME, "discovery records")?;
        let mut encoded = vec![0u8; length];
        turn.reader()
            .read_exact(&mut encoded)
            .await
            .map_err(transport_io)?;
        let records = crate::sync::wire::decode_records(
            &encoded,
            MAX_TRANSFER_REQUEST_OBJECTS,
            MAX_CONTROL_FRAME,
        )
        .map_err(|error| TransferError::SourceTransport(error.to_string()))?;

        let (status, length) = read_response_header(turn.reader()).await?;
        if status != STATUS_OK {
            return Err(TransferError::SourceTransport(
                "invalid discovery payload key list".into(),
            ));
        }
        let length = bounded_length(length, MAX_CONTROL_FRAME, "discovery payload keys")?;
        let mut encoded = vec![0u8; length];
        turn.reader()
            .read_exact(&mut encoded)
            .await
            .map_err(transport_io)?;
        let payload_keys = decode_object_keys(&encoded).map_err(TransferError::SourceTransport)?;

        let (status, count) = read_response_header(turn.reader()).await?;
        if status != STATUS_OK
            || count != payload_keys.len() as u64
            || payload_keys.len() > MAX_PAYLOAD_BATCH_OBJECTS
        {
            return Err(TransferError::SourceTransport(
                "invalid discovery payload count".into(),
            ));
        }
        let by_key: BTreeMap<&ObjectKey, &ObjectRecord> = records
            .iter()
            .map(|record| (record.key(), record))
            .collect();
        let mut payloads = Vec::with_capacity(payload_keys.len());
        let mut remaining = DISCOVERY_ANSWER_PAYLOAD_BYTES as u64;
        for key in payload_keys {
            let Some(record) = by_key.get(&key) else {
                return Err(TransferError::SourceTransport(
                    "discovery payload for a key without a record".into(),
                ));
            };
            let (status, length) = read_response_header(turn.reader()).await?;
            if status != STATUS_OK || length != record.payload_size() || length > remaining {
                return Err(TransferError::SourceTransport(
                    "invalid discovery payload length".into(),
                ));
            }
            remaining -= length;
            let payload = receive_batched_frame(turn.boxed(), record, sources).await?;
            payloads.push((key, payload));
        }
        turn.finish();
        Ok(Some(TransferDiscoveryAnswer { records, payloads }))
    }

    async fn payload_batch(
        &self,
        records: &[ObjectRecord],
        max_bytes: usize,
        sources: &dyn SliceSources,
    ) -> Result<Option<Vec<Option<TransferBatchPayload>>>, TransferError> {
        let limit = max_bytes.min(MAX_PAYLOAD_BATCH_BYTES) as u64;
        let size = records
            .iter()
            .try_fold(0u64, |sum, record| sum.checked_add(record.payload_size()));
        if !self.supports_payload_batch()
            || records.len() > MAX_PAYLOAD_BATCH_OBJECTS
            || size.is_none_or(|size| size > limit)
        {
            return Ok(None);
        }
        if records.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let request =
            encode_object_keys(&records.iter().map(|r| r.key().clone()).collect::<Vec<_>>())?;
        let ticket = self.connection.send(OP_PAYLOADS, &request).await?;
        // Any cancellation, short read, or invalid frame leaves the connection
        // poisoned until the entire batch has been consumed successfully.
        let mut turn = self.connection.receive(ticket).await?;
        let (status, count) = read_response_header(turn.reader()).await?;
        if status == STATUS_FALLBACK && count == 0 {
            turn.finish();
            return Ok(None);
        }
        if status != STATUS_OK || count != records.len() as u64 {
            return Err(TransferError::SourceTransport(
                "invalid payload batch response".into(),
            ));
        }
        let mut readers = Vec::with_capacity(records.len());
        for record in records {
            let (status, length) = read_response_header(turn.reader()).await?;
            if status != STATUS_OK || length != record.payload_size() {
                return Err(TransferError::SourceTransport(format!(
                    "invalid batched payload for {}",
                    record.key()
                )));
            }
            readers.push(receive_batched_frame(turn.boxed(), record, sources).await?);
        }
        turn.finish();
        Ok(Some(readers))
    }

    async fn open_payload(
        &self,
        record: &ObjectRecord,
    ) -> Result<Option<TransferPayloadReader>, TransferError> {
        let ticket = self
            .connection
            .send(OP_PAYLOAD, &record.key().encode())
            .await?;
        let mut turn = self.connection.receive(ticket).await?;
        let (status, length) = read_response_header(turn.reader()).await?;
        match status {
            STATUS_OK => {
                if length != record.payload_size() {
                    return Err(TransferError::SourceTransport(format!(
                        "remote payload for {} has length {length}, expected {}",
                        record.key(),
                        record.payload_size()
                    )));
                }
                Ok(Some(decoded_payload_reader(turn, record.payload(), length)))
            }
            STATUS_ABSENT if length == 0 => {
                turn.finish();
                Ok(None)
            }
            STATUS_ERROR => {
                let error = read_remote_error(turn.reader(), length).await;
                turn.finish();
                Err(error)
            }
            _ => Err(TransferError::SourceTransport(
                "invalid remote payload response status".to_owned(),
            )),
        }
    }

    async fn open_proof(
        &self,
        record: &ObjectRecord,
    ) -> Result<Option<TransferPayloadReader>, TransferError> {
        let expected = proof_length(record.payload_size()).map_err(transport_io)?;
        let ticket = self
            .connection
            .send(OP_PROOF, &record.key().encode())
            .await?;
        let mut turn = self.connection.receive(ticket).await?;
        let (status, length) = read_response_header(turn.reader()).await?;
        match status {
            STATUS_OK if length == expected => {
                if length == 0 {
                    // Nothing follows the header; the turn is already over.
                    turn.finish();
                    return Ok(Some(Box::new(std::io::Cursor::new(Vec::new()))));
                }
                Ok(Some(Box::new(RemotePayloadReader {
                    turn: Some(turn),
                    remaining: length,
                })))
            }
            STATUS_ABSENT if length == 0 => {
                turn.finish();
                Ok(None)
            }
            STATUS_ERROR => {
                let error = read_remote_error(turn.reader(), length).await;
                turn.finish();
                Err(error)
            }
            _ => Err(TransferError::SourceTransport(
                "invalid Bao stream response".into(),
            )),
        }
    }

    async fn offer_slice_bases(
        &self,
        pairs: &[SliceBasePair],
    ) -> Result<Option<SliceBases>, TransferError> {
        if pairs.is_empty() {
            return Ok(None);
        }
        // Held and wanted keys alternate in one bounded key list.
        let keys: Vec<ObjectKey> = pairs
            .iter()
            .flat_map(|pair| [pair.held.clone(), pair.wanted.clone()])
            .collect();
        let request = encode_object_keys(&keys)?;
        let ticket = self.connection.send(OP_SLICE_BASES, &request).await?;
        // The answer carries only a count, so it is consumed in turn by a task
        // and the transfer's next request goes out without waiting for it.
        let connection = self.connection.clone();
        tokio::spawn(async move {
            let Ok(mut turn) = connection.receive(ticket).await else {
                return;
            };
            let Ok((status, length)) = read_response_header(turn.reader()).await else {
                return;
            };
            match status {
                STATUS_OK if length == 8 => {
                    let mut hints = [0u8; 8];
                    if turn.reader().read_exact(&mut hints).await.is_ok() {
                        tracing::debug!(
                            hints = u64::from_le_bytes(hints),
                            "source paired offered closures as slice bases"
                        );
                        turn.finish();
                    }
                }
                STATUS_ERROR => {
                    let error = read_remote_error(turn.reader(), length).await;
                    tracing::warn!(%error, "source rejected offered slice bases");
                    turn.finish();
                }
                _ => {}
            }
        });
        Ok(Some(SliceBases::default()))
    }

    async fn receive_sliced(
        &self,
        record: &ObjectRecord,
        sources: &dyn SliceSources,
        sink: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> Result<SlicedReceipt, TransferError> {
        let ticket = self
            .connection
            .send(OP_SLICED, &record.key().encode())
            .await?;
        // The frame is self-delimiting: a decode failure before its end
        // leaves the stream unusable, while a missing source consumes it.
        let mut turn = self.connection.receive(ticket).await?;
        let (status, length) = read_response_header(turn.reader()).await?;
        match status {
            STATUS_OK => {
                if length != record.payload_size() {
                    return Err(TransferError::SourceTransport(format!(
                        "remote sliced payload for {} declares {length} bytes, expected {}",
                        record.key(),
                        record.payload_size()
                    )));
                }
                match decode_sliced(
                    turn.boxed(),
                    record.payload(),
                    record.payload_size(),
                    sources,
                    sink,
                )
                .await
                {
                    Ok(stats) => {
                        turn.finish();
                        Ok(SlicedReceipt::Received(stats))
                    }
                    Err(SliceError::MissingSource(source)) => {
                        turn.finish();
                        Ok(SlicedReceipt::MissingSource(source))
                    }
                    Err(error) => Err(TransferError::SourceTransport(format!(
                        "sliced payload for {}: {error}",
                        record.key()
                    ))),
                }
            }
            STATUS_ABSENT if length == 0 => {
                turn.finish();
                Ok(SlicedReceipt::Absent)
            }
            STATUS_ERROR => {
                let error = read_remote_error(turn.reader(), length).await;
                turn.finish();
                Err(error)
            }
            _ => Err(TransferError::SourceTransport(
                "invalid sliced payload response".into(),
            )),
        }
    }
}

fn proof_length(size: u64) -> std::io::Result<u64> {
    size.checked_add(bao_tree::BaoTree::new(size, crate::verified::BLOCK_SIZE).outboard_size())
        .ok_or_else(|| std::io::Error::other("Bao wire length overflows"))
}

fn decode_requested_records(
    keys: &[ObjectKey],
    encoded: &[u8],
) -> Result<Vec<Option<ObjectRecord>>, TransferError> {
    let records = crate::sync::wire::decode_records(encoded, keys.len(), MAX_CONTROL_FRAME)
        .map_err(|error| TransferError::SourceTransport(error.to_string()))?;
    let requested: BTreeSet<_> = keys.iter().cloned().collect();
    let mut by_key = BTreeMap::<ObjectKey, ObjectRecord>::new();
    for record in records {
        if !requested.contains(record.key()) {
            return Err(TransferError::SourceTransport(format!(
                "remote batch returned unrequested record {}",
                record.key()
            )));
        }
        if let Some(previous) = by_key.insert(record.key().clone(), record.clone())
            && previous != record
        {
            return Err(TransferError::SourceTransport(format!(
                "remote batch returned conflicting records for {}",
                record.key()
            )));
        }
    }
    Ok(keys.iter().map(|key| by_key.get(key).cloned()).collect())
}

/// One request/response stream with pipelining: requests are written under
/// the writer lock and numbered, and each answer is consumed by its ticket
/// holder in that order under the reader lock, so several requests can be
/// in flight and the server answers them back to back.
struct RpcConnection {
    writer: Mutex<RpcWriter>,
    reader: Arc<Mutex<RpcReader>>,
    turn: tokio::sync::Notify,
    poisoned: std::sync::atomic::AtomicBool,
    child: std::sync::Mutex<Option<Child>>,
    requests: AtomicU64,
    operations: std::sync::Mutex<BTreeMap<u8, u64>>,
}

struct RpcWriter {
    writer: Box<dyn AsyncWrite + Send + Unpin>,
    next_ticket: u64,
}

struct RpcReader {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    served: u64,
}

impl RpcConnection {
    fn new(
        reader: Box<dyn AsyncRead + Send + Unpin>,
        writer: Box<dyn AsyncWrite + Send + Unpin>,
        child: Option<Child>,
    ) -> Self {
        Self {
            writer: Mutex::new(RpcWriter {
                writer,
                next_ticket: 0,
            }),
            reader: Arc::new(Mutex::new(RpcReader { reader, served: 0 })),
            turn: tokio::sync::Notify::new(),
            poisoned: std::sync::atomic::AtomicBool::new(false),
            child: std::sync::Mutex::new(child),
            requests: AtomicU64::new(0),
            operations: std::sync::Mutex::new(BTreeMap::new()),
        }
    }

    fn ensure_usable(&self) -> Result<(), TransferError> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(TransferError::SourceTransport(
                "remote source connection is no longer usable".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    /// Mark the stream unusable, stop the remote process, and wake every
    /// ticket holder so it fails instead of waiting for a turn that never
    /// comes.
    fn poison(&self) {
        self.poisoned.store(true, Ordering::Release);
        if let Some(child) = &mut *self.child.lock().expect("child slot") {
            let _ = child.start_kill();
        }
        self.turn.notify_waiters();
    }

    fn count(&self, operation: u8) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        *self
            .operations
            .lock()
            .expect("operation counters")
            .entry(operation)
            .or_insert(0) += 1;
    }

    /// Write one request and return its ticket.
    async fn send(&self, operation: u8, body: &[u8]) -> Result<u64, TransferError> {
        self.ensure_usable()?;
        let mut writer = self.writer.lock().await;
        let ticket = writer.next_ticket;
        writer.next_ticket += 1;
        self.count(operation);
        if let Err(error) = write_request(&mut writer.writer, operation, body).await {
            self.poison();
            return Err(error);
        }
        Ok(ticket)
    }

    /// Write several requests back to back, handing the first ticket to the
    /// caller before the first byte goes out so answers can be consumed
    /// while a bounded transport still blocks the later requests.
    async fn send_pipelined(
        &self,
        requests: &[(u8, Vec<u8>)],
        first: tokio::sync::oneshot::Sender<u64>,
    ) -> Result<(), TransferError> {
        self.ensure_usable()?;
        let mut writer = self.writer.lock().await;
        let ticket = writer.next_ticket;
        writer.next_ticket += requests.len() as u64;
        let _ = first.send(ticket);
        for (operation, body) in requests {
            self.count(*operation);
            if let Err(error) = write_request(&mut writer.writer, *operation, body).await {
                self.poison();
                return Err(error);
            }
        }
        Ok(())
    }

    /// Wait for the answer to `ticket` to be next on the stream and take the
    /// reader for it.
    async fn receive(self: &Arc<Self>, ticket: u64) -> Result<Turn, TransferError> {
        loop {
            let notified = self.turn.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.ensure_usable()?;
            let guard = self.reader.clone().lock_owned().await;
            if guard.served == ticket {
                return Ok(Turn {
                    guard: Some(guard),
                    connection: self.clone(),
                    done: false,
                });
            }
            drop(guard);
            notified.await;
        }
    }
}

/// The reader held while one answer is consumed. Finishing hands the stream
/// to the next ticket; dropping without finishing leaves an unread tail, so
/// it poisons the connection.
struct Turn {
    guard: Option<OwnedMutexGuard<RpcReader>>,
    connection: Arc<RpcConnection>,
    done: bool,
}

impl Turn {
    fn boxed(&mut self) -> &mut Box<dyn AsyncRead + Send + Unpin> {
        &mut self.guard.as_mut().expect("turn holds the reader").reader
    }

    fn reader(&mut self) -> &mut (dyn AsyncRead + Send + Unpin) {
        &mut **self.boxed()
    }

    fn finish(mut self) {
        self.done = true;
        if let Some(mut guard) = self.guard.take() {
            guard.served += 1;
            drop(guard);
        }
        self.connection.turn.notify_waiters();
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        if !self.done {
            self.connection.poison();
        }
    }
}

/// Decode one batched sliced frame into memory. `None` means the frame named
/// a source the receiver lacks; it was consumed, so the stream stays aligned
/// and that payload can be fetched individually.
async fn receive_batched_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    record: &ObjectRecord,
    sources: &dyn SliceSources,
) -> Result<Option<TransferBatchPayload>, TransferError> {
    let mut plaintext = Vec::with_capacity(record.payload_size() as usize);
    match decode_sliced(
        reader,
        record.payload(),
        record.payload_size(),
        sources,
        &mut plaintext,
    )
    .await
    {
        Ok(stats) => Ok(Some((Box::new(std::io::Cursor::new(plaintext)), stats))),
        Err(SliceError::MissingSource(_)) => Ok(None),
        Err(error) => Err(TransferError::SourceTransport(format!(
            "batched payload for {}: {error}",
            record.key()
        ))),
    }
}

/// Plaintext bytes decoded per poll while streaming one payload frame.
const DECODED_PAYLOAD_BUFFER: usize = 256 * 1024;

/// Stream the plaintext of a literal-only frame arriving on `turn`. A decode
/// task drains the frame into a bounded pipe, so the caller reads plaintext
/// at its own pace while the server stays blocked on backpressure. The turn
/// is finished only after the frame ended cleanly; any failure, or dropping
/// the reader early, poisons the connection.
fn decoded_payload_reader(
    mut turn: Turn,
    payload: crate::BlobId,
    size: u64,
) -> TransferPayloadReader {
    let (mut sink, output) = tokio::io::duplex(DECODED_PAYLOAD_BUFFER);
    let failure = Arc::new(std::sync::Mutex::new(None));
    let failed = failure.clone();
    tokio::spawn(async move {
        let outcome = decode_sliced(turn.boxed(), payload, size, &NoSources, &mut sink).await;
        match outcome {
            Ok(_) => {
                turn.finish();
                let _ = sink.shutdown().await;
            }
            Err(error) => {
                *failed.lock().expect("payload failure slot") = Some(error.to_string());
                drop(turn);
            }
        }
    });
    Box::new(DecodedPayloadReader { output, failure })
}

struct DecodedPayloadReader {
    output: tokio::io::DuplexStream,
    failure: Arc<std::sync::Mutex<Option<String>>>,
}

impl AsyncRead for DecodedPayloadReader {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let before = output.filled().len();
        match Pin::new(&mut this.output).poll_read(context, output) {
            Poll::Ready(Ok(())) if output.filled().len() == before && output.remaining() > 0 => {
                // EOF: either the frame ended cleanly or the decoder failed.
                match this.failure.lock().expect("payload failure slot").take() {
                    Some(message) => Poll::Ready(Err(std::io::Error::other(message))),
                    None => Poll::Ready(Ok(())),
                }
            }
            other => other,
        }
    }
}

/// A raw length-bounded stream, used for Bao proofs. Reading it to the end
/// finishes its turn; stopping early poisons the connection.
struct RemotePayloadReader {
    turn: Option<Turn>,
    remaining: u64,
}

impl AsyncRead for RemotePayloadReader {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.remaining == 0 || output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let Some(turn) = this.turn.as_mut() else {
            return Poll::Ready(Err(std::io::Error::other("payload stream already failed")));
        };
        let limit = output
            .remaining()
            .min(usize::try_from(this.remaining).unwrap_or(usize::MAX));
        let unfilled = output.initialize_unfilled_to(limit);
        let mut bounded = ReadBuf::new(unfilled);
        match Pin::new(turn.reader()).poll_read(context, &mut bounded) {
            Poll::Ready(Ok(())) => {
                let read = bounded.filled().len();
                if read == 0 {
                    // Dropping the turn unfinished poisons the connection.
                    this.turn = None;
                    this.remaining = 0;
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "SSH source ended inside a payload",
                    )));
                }
                output.advance(read);
                this.remaining -= read as u64;
                if this.remaining == 0
                    && let Some(turn) = this.turn.take()
                {
                    turn.finish();
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                this.turn = None;
                this.remaining = 0;
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn write_request(
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    operation: u8,
    body: &[u8],
) -> Result<(), TransferError> {
    if body.len() > MAX_CONTROL_FRAME {
        return Err(TransferError::SourceTransport(format!(
            "source request exceeds {MAX_CONTROL_FRAME} bytes"
        )));
    }
    let mut header = [0u8; REQUEST_HEADER_LEN];
    header[0] = operation;
    header[1..].copy_from_slice(&(body.len() as u64).to_le_bytes());
    writer.write_all(&header).await.map_err(transport_io)?;
    writer.write_all(body).await.map_err(transport_io)?;
    writer.flush().await.map_err(transport_io)
}

async fn read_response_header(
    reader: &mut (dyn AsyncRead + Send + Unpin),
) -> Result<(u8, u64), TransferError> {
    let mut header = [0u8; RESPONSE_HEADER_LEN];
    reader.read_exact(&mut header).await.map_err(transport_io)?;
    Ok((
        header[0],
        u64::from_le_bytes(header[1..].try_into().expect("header width is fixed")),
    ))
}

async fn read_remote_error(
    reader: &mut (dyn AsyncRead + Send + Unpin),
    length: u64,
) -> TransferError {
    let Ok(length) = bounded_length(length, MAX_ERROR_FRAME, "remote error") else {
        return TransferError::SourceTransport("remote error frame exceeds its limit".to_owned());
    };
    let mut encoded = vec![0u8; length];
    if let Err(error) = reader.read_exact(&mut encoded).await {
        return transport_io(error);
    }
    TransferError::SourceTransport(String::from_utf8_lossy(&encoded).into_owned())
}

fn bounded_length(length: u64, limit: usize, what: &str) -> Result<usize, TransferError> {
    let length = usize::try_from(length).map_err(|_| {
        TransferError::SourceTransport(format!("{what} length does not fit this platform"))
    })?;
    if length > limit {
        Err(TransferError::SourceTransport(format!(
            "{what} is {length} bytes, limit is {limit}"
        )))
    } else {
        Ok(length)
    }
}

fn transport_io(error: std::io::Error) -> TransferError {
    TransferError::SourceTransport(error.to_string())
}

/// Serve one stable repository snapshot over the bounded SSH stdio protocol.
///
/// Callers are expected to expose this only behind an authenticated encrypted
/// transport. [`SshTransferSource`] invokes it through the hidden Casita CLI
/// source process after OpenSSH has authenticated both ends.
#[tracing::instrument(name = "ssh.serve_stdio", skip_all)]
pub async fn serve_transfer_stdio<PS, SS, R, W>(
    repository: &Repository<PS, SS>,
    mut input: R,
    mut output: W,
) -> Result<(), SshTransportError>
where
    PS: BlobStore,
    SS: MetadataStore,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut hold = repository.retention_hold().await?;
    let mut selection_allowed = true;
    // Wanted payload key to the held payload key at the same path, filled by
    // the receiver's base offers; each held blob is indexed when first needed.
    let mut slice_hints: BTreeMap<ObjectKey, ObjectKey> = BTreeMap::new();
    let mut slice_index = SliceIndex::new(SLICE_INDEX_MAX_ENTRIES);
    // Wanted keys whose subtree is identical to a held one; discovery neither
    // descends into them nor volunteers their payloads.
    let mut slice_held: BTreeSet<ObjectKey> = BTreeSet::new();
    let slice_sources = BlobSliceSources::new(repository.payloads());
    tracing::info!(
        revision = %hold.snapshot().revision(),
        "SSH transfer source serving snapshot"
    );
    output.write_all(PROTOCOL_MAGIC).await?;
    output
        .write_all(hold.snapshot().revision().as_bytes())
        .await?;
    output.flush().await?;

    loop {
        let Some(operation) = input.read_u8().await.optional_eof()? else {
            return Ok(());
        };
        let length = input.read_u64_le().await?;
        let length = match usize::try_from(length) {
            Ok(length) if length <= MAX_CONTROL_FRAME => length,
            _ => {
                write_error(&mut output, "request frame exceeds its limit").await?;
                return Err(SshTransportError::Protocol(
                    "request frame exceeds its limit".to_owned(),
                ));
            }
        };
        let mut body = vec![0u8; length];
        input.read_exact(&mut body).await?;
        tracing::debug!(
            operation,
            request_bytes = length,
            "SSH source request received"
        );

        if operation != OP_SELECTION {
            selection_allowed = false;
        }
        match operation {
            OP_SELECTION => {
                if !selection_allowed {
                    write_error(
                        &mut output,
                        "selection must precede all reads and may only be sent once",
                    )
                    .await?;
                    continue;
                }
                selection_allowed = false;
                match decode_selection(&body) {
                    Ok(selection) => match selection.apply_to(&mut hold).await {
                        Ok(()) => write_control(&mut output, &[]).await?,
                        Err(error) => write_error(&mut output, &error.to_string()).await?,
                    },
                    Err(error) => write_error(&mut output, &error).await?,
                }
            }

            OP_OBJECT => match decode_key(&body) {
                Ok(key) => match hold.object(&key).await {
                    Ok(Some(record)) => write_control(&mut output, &record.encode()).await?,
                    Ok(None) => write_absent(&mut output).await?,
                    Err(error) => write_error(&mut output, &error.to_string()).await?,
                },
                Err(error) => write_error(&mut output, &error).await?,
            },
            OP_ROOT => match decode_root_name(&body) {
                Ok(name) => match hold.snapshot().root(&name).await {
                    Ok(Some(key)) => write_control(&mut output, &key.encode()).await?,
                    Ok(None) => write_absent(&mut output).await?,
                    Err(error) => write_error(&mut output, &error.to_string()).await?,
                },
                Err(error) => write_error(&mut output, &error).await?,
            },
            OP_PAYLOAD => match decode_key(&body) {
                Ok(key) => match hold.open_payload(&key).await {
                    Ok(Some((record, reader))) => {
                        // A single payload answer is literal only: the client
                        // decodes it without access to its own store.
                        write_response_header(&mut output, STATUS_OK, record.payload_size())
                            .await?;
                        if let Err(error) = encode_literal_stream(
                            record.payload(),
                            record.payload_size(),
                            reader,
                            &mut output,
                        )
                        .await
                        {
                            return Err(SshTransportError::Protocol(format!(
                                "payload for {key} failed: {error}"
                            )));
                        }
                        output.flush().await?;
                    }
                    Ok(None) => write_absent(&mut output).await?,
                    Err(error) => write_error(&mut output, &error.to_string()).await?,
                },
                Err(error) => write_error(&mut output, &error).await?,
            },
            OP_PROOF => match decode_key(&body) {
                Ok(key) => match hold.object(&key).await? {
                    Some(record) => {
                        match repository
                            .payloads()
                            .open_proof(&record.payload(), record.payload_size())
                            .await
                        {
                            Ok(Some(mut reader)) => {
                                let length = proof_length(record.payload_size())?;
                                write_response_header(&mut output, STATUS_OK, length).await?;
                                let copied = tokio::io::copy(&mut reader, &mut output).await?;
                                if copied != length {
                                    return Err(SshTransportError::Protocol(
                                        "Bao stream length mismatch".into(),
                                    ));
                                }
                                output.flush().await?;
                            }
                            Ok(None) => write_absent(&mut output).await?,
                            Err(error) => write_error(&mut output, &error.to_string()).await?,
                        }
                    }
                    None => write_absent(&mut output).await?,
                },
                Err(error) => write_error(&mut output, &error).await?,
            },
            OP_PAYLOADS => {
                let keys = decode_object_keys(&body).map_err(SshTransportError::Protocol)?;
                if keys.len() > MAX_PAYLOAD_BATCH_OBJECTS {
                    write_fallback(&mut output).await?;
                    continue;
                }
                let mut records = Vec::with_capacity(keys.len());
                let mut total = 0u64;
                for key in &keys {
                    let Some(record) = hold.object(key).await? else {
                        break;
                    };
                    total = total.saturating_add(record.payload_size());
                    if total > MAX_PAYLOAD_BATCH_BYTES as u64 {
                        break;
                    }
                    records.push(record);
                }
                if records.len() != keys.len() {
                    write_fallback(&mut output).await?;
                    continue;
                }
                write_response_header(&mut output, STATUS_OK, records.len() as u64).await?;
                for record in records {
                    // Batched answers slice against paired counterparts too,
                    // so small rebuilt files cost as little as large ones.
                    if let Err(error) =
                        index_slice_hint(&hold, &mut slice_index, &slice_hints, record.key()).await
                    {
                        return Err(SshTransportError::Protocol(format!(
                            "batched payload for {} failed: {error}",
                            record.key()
                        )));
                    }
                    let Some((_, reader)) = hold.open_payload(record.key()).await? else {
                        return Err(SshTransportError::Protocol(
                            "missing batched payload".into(),
                        ));
                    };
                    write_response_header(&mut output, STATUS_OK, record.payload_size()).await?;
                    let encoded = if slice_index.entries() > 0 {
                        encode_sliced_stream(
                            record.payload(),
                            record.payload_size(),
                            reader,
                            &slice_index,
                            &slice_sources,
                            &mut output,
                        )
                        .await
                    } else {
                        encode_literal_stream(
                            record.payload(),
                            record.payload_size(),
                            reader,
                            &mut output,
                        )
                        .await
                    };
                    if let Err(error) = encoded {
                        return Err(SshTransportError::Protocol(format!(
                            "batched payload for {} failed: {error}",
                            record.key()
                        )));
                    }
                }
                output.flush().await?;
            }
            OP_OBJECTS => match decode_object_keys(&body) {
                Ok(keys) => {
                    let mut records = BTreeMap::new();
                    let mut failure = None;
                    for key in keys {
                        match hold.object(&key).await {
                            Ok(Some(record)) => {
                                records.insert(key, record);
                            }
                            Ok(None) => {}
                            Err(error) => {
                                failure = Some(error.to_string());
                                break;
                            }
                        }
                    }
                    if let Some(error) = failure {
                        write_error(&mut output, &error).await?;
                    } else {
                        let records: Vec<_> = records.into_values().collect();
                        write_control(&mut output, &crate::sync::wire::encode_records(&records))
                            .await?;
                    }
                }
                Err(error) => write_error(&mut output, &error).await?,
            },
            OP_PATH_PROOF => match crate::sync::wire::decode_path_proof_request(&body) {
                Ok((root, components)) => match build_path_proof(&hold, &root, &components).await {
                    Ok(ServerPathProof::MissingRoot) => write_absent(&mut output).await?,
                    Ok(ServerPathProof::Fallback) => write_fallback(&mut output).await?,
                    Ok(ServerPathProof::Proof(proof)) => {
                        let encoded = crate::sync::wire::encode_path_proof(&proof);
                        if encoded.len() > MAX_CONTROL_FRAME {
                            write_fallback(&mut output).await?;
                        } else {
                            write_control(&mut output, &encoded).await?;
                        }
                    }
                    Err(error) => write_error(&mut output, &error).await?,
                },
                Err(error) => write_error(&mut output, &error.to_string()).await?,
            },
            OP_SLICE_BASES => match decode_object_keys(&body) {
                Ok(keys) if keys.len().is_multiple_of(2) => {
                    let pairs: Vec<(ObjectKey, ObjectKey)> = keys
                        .chunks_exact(2)
                        .map(|pair| (pair[0].clone(), pair[1].clone()))
                        .collect();
                    match pair_slice_hints(
                        &hold,
                        &mut slice_hints,
                        &mut slice_held,
                        &pairs,
                        repository.limits().max_metadata_bytes,
                    )
                    .await
                    {
                        Ok(()) => {
                            tracing::debug!(
                                pairs = pairs.len(),
                                hints = slice_hints.len(),
                                "paired offered closures by path as slice bases"
                            );
                            write_control(&mut output, &(slice_hints.len() as u64).to_le_bytes())
                                .await?
                        }
                        Err(error) => write_error(&mut output, &error.to_string()).await?,
                    }
                }
                Ok(_) => write_error(&mut output, "slice bases must come in pairs").await?,
                Err(error) => write_error(&mut output, &error).await?,
            },
            OP_SLICED => match decode_key(&body) {
                Ok(key) => match hold.open_payload(&key).await {
                    Ok(Some((record, reader))) => {
                        if let Err(error) =
                            index_slice_hint(&hold, &mut slice_index, &slice_hints, &key).await
                        {
                            write_error(&mut output, &error.to_string()).await?;
                            continue;
                        }
                        write_response_header(&mut output, STATUS_OK, record.payload_size())
                            .await?;
                        let encoded = if slice_index.entries() > 0 {
                            encode_sliced_stream(
                                record.payload(),
                                record.payload_size(),
                                reader,
                                &slice_index,
                                &slice_sources,
                                &mut output,
                            )
                            .await
                        } else {
                            encode_literal_stream(
                                record.payload(),
                                record.payload_size(),
                                reader,
                                &mut output,
                            )
                            .await
                        };
                        match encoded {
                            Ok(stats) => tracing::debug!(
                                copies = stats.copies,
                                copy_bytes = stats.copy_bytes,
                                literal_bytes = stats.literal_bytes,
                                frame_bytes = stats.frame_bytes,
                                "served sliced payload"
                            ),
                            Err(error) => {
                                return Err(SshTransportError::Protocol(format!(
                                    "sliced payload for {key} failed: {error}"
                                )));
                            }
                        }
                        output.flush().await?;
                    }
                    Ok(None) => write_absent(&mut output).await?,
                    Err(error) => write_error(&mut output, &error.to_string()).await?,
                },
                Err(error) => write_error(&mut output, &error).await?,
            },
            OP_DISCOVER => match decode_object_keys(&body) {
                Ok(keys) if !keys.is_empty() && keys.len() <= MAX_TRANSFER_REQUEST_OBJECTS => {
                    let Some(answer) = discover_closure(&hold, &slice_held, &keys).await? else {
                        write_fallback(&mut output).await?;
                        continue;
                    };
                    let records = crate::sync::wire::encode_records(&answer.records);
                    let payload_keys = encode_object_keys(
                        &answer
                            .payloads
                            .iter()
                            .map(|record| record.key().clone())
                            .collect::<Vec<_>>(),
                    )
                    .map_err(|error| SshTransportError::Protocol(error.to_string()))?;
                    write_response_header(&mut output, STATUS_OK, records.len() as u64).await?;
                    output.write_all(&records).await?;
                    write_response_header(&mut output, STATUS_OK, payload_keys.len() as u64)
                        .await?;
                    output.write_all(&payload_keys).await?;
                    write_response_header(&mut output, STATUS_OK, answer.payloads.len() as u64)
                        .await?;
                    for record in &answer.payloads {
                        if let Err(error) =
                            index_slice_hint(&hold, &mut slice_index, &slice_hints, record.key())
                                .await
                        {
                            return Err(SshTransportError::Protocol(format!(
                                "discovery payload for {} failed: {error}",
                                record.key()
                            )));
                        }
                        let Some((_, reader)) = hold.open_payload(record.key()).await? else {
                            return Err(SshTransportError::Protocol(
                                "missing discovery payload".into(),
                            ));
                        };
                        write_response_header(&mut output, STATUS_OK, record.payload_size())
                            .await?;
                        let encoded = if slice_index.entries() > 0 {
                            encode_sliced_stream(
                                record.payload(),
                                record.payload_size(),
                                reader,
                                &slice_index,
                                &slice_sources,
                                &mut output,
                            )
                            .await
                        } else {
                            encode_literal_stream(
                                record.payload(),
                                record.payload_size(),
                                reader,
                                &mut output,
                            )
                            .await
                        };
                        if let Err(error) = encoded {
                            return Err(SshTransportError::Protocol(format!(
                                "discovery payload for {} failed: {error}",
                                record.key()
                            )));
                        }
                    }
                    output.flush().await?;
                    tracing::debug!(
                        requested = keys.len(),
                        records = answer.records.len(),
                        payloads = answer.payloads.len(),
                        "answered discovery with the reachable closure"
                    );
                }
                Ok(_) => write_error(&mut output, "discovery keys outside their bounds").await?,
                Err(error) => write_error(&mut output, &error).await?,
            },
            _ => write_error(&mut output, "unknown source operation").await?,
        }
    }
}

/// A discovery answer before encoding: records breadth first from the
/// requested keys, and the records whose payloads travel with them.
struct DiscoveredClosure {
    records: Vec<ObjectRecord>,
    payloads: Vec<ObjectRecord>,
}

/// Collect the requested records and as many descendants as fit one answer,
/// withholding payloads of subtrees the receiver holds. `None` when even the
/// requested records do not fit, so the client keeps its batched path.
async fn discover_closure<PS, SS>(
    hold: &RetentionHold<'_, PS, SS>,
    held: &BTreeSet<ObjectKey>,
    keys: &[ObjectKey],
) -> Result<Option<DiscoveredClosure>, RepositoryError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    // Leave room for the frame's own count and per-record lengths.
    let record_budget = MAX_CONTROL_FRAME - 64 * 1024;
    let mut records = Vec::new();
    let mut withheld = BTreeSet::new();
    let mut encoded = 0usize;
    let mut visited = BTreeSet::new();
    let mut queue: std::collections::VecDeque<(ObjectKey, bool, bool)> = keys
        .iter()
        .map(|key| (key.clone(), true, held.contains(key)))
        .collect();
    while let Some((key, requested, inherited)) = queue.pop_front() {
        if !visited.insert(key.clone()) {
            continue;
        }
        let Some(record) = hold.object(&key).await? else {
            continue;
        };
        let size = record.encode().len() + 8;
        if records.len() >= MAX_TRANSFER_REQUEST_OBJECTS || encoded + size > record_budget {
            if requested {
                return Ok(None);
            }
            break;
        }
        encoded += size;
        // Held subtrees are still described: a receiver discovering the whole
        // closure asks for their records anyway, and records are cheap. Their
        // payloads are withheld, and so are their descendants': a subtree the
        // receiver holds is held entire.
        let holds = inherited || held.contains(&key);
        if holds {
            withheld.insert(key.clone());
        }
        queue.extend(
            record
                .links()
                .iter()
                .map(|link| (link.clone(), false, holds)),
        );
        records.push(record);
    }
    // A receiver that offered no bases holds nothing, so everything this
    // answer volunteers is wanted. One that offered bases holds a whole
    // previous generation that the pairing covers only in part, so a guess
    // made while it walks that closure is made blind: on a rebuilt 349-path
    // closure it delivered 957 payloads, 17.9 MB of plaintext, that the
    // receiver threw away, 6.6 MB of it on the wire.
    let volunteer = held.is_empty() || keys.len() <= VOLUNTEER_MAX_REQUEST_KEYS;
    let mut payloads = Vec::new();
    let mut bytes = 0u64;
    for record in records.iter().filter(|_| volunteer) {
        if record.payload_size() == 0 || withheld.contains(record.key()) {
            continue;
        }
        if payloads.len() == DISCOVERY_ANSWER_PAYLOAD_OBJECTS
            || bytes + record.payload_size() > DISCOVERY_ANSWER_PAYLOAD_BYTES as u64
        {
            break;
        }
        bytes += record.payload_size();
        payloads.push(record.clone());
    }
    Ok(Some(DiscoveredClosure { records, payloads }))
}

/// Decode one directory record's payload from the held snapshot, or `None`
/// when it is absent or not a directory the serving limits allow.
async fn read_held_directory<PS, SS>(
    hold: &RetentionHold<'_, PS, SS>,
    key: &ObjectKey,
    limit: u64,
) -> Result<Option<Directory>, RepositoryError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    if key.namespace().as_str() != crate::DIRECTORY_NAMESPACE {
        return Ok(None);
    }
    let Some((record, reader)) = hold.open_payload(key).await? else {
        return Ok(None);
    };
    if record.payload_size() > limit {
        return Ok(None);
    }
    let mut encoded = Vec::with_capacity(record.payload_size() as usize);
    reader
        .take(record.payload_size())
        .read_to_end(&mut encoded)
        .await?;
    Ok(Directory::decode(&encoded).ok())
}

/// Walk each held and wanted tree in lockstep and record, for every wanted
/// blob, the held blob at the same path. Only differing entries matter: an
/// identical payload is already present at the receiver, and identical
/// subtrees need no descent.
async fn pair_slice_hints<PS, SS>(
    hold: &RetentionHold<'_, PS, SS>,
    hints: &mut BTreeMap<ObjectKey, ObjectKey>,
    held: &mut BTreeSet<ObjectKey>,
    pairs: &[(ObjectKey, ObjectKey)],
    directory_limit: u64,
) -> Result<(), RepositoryError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    for (from, to) in pairs {
        if from == to {
            held.insert(to.clone());
        }
    }
    let mut queue: std::collections::VecDeque<(ObjectKey, ObjectKey)> = pairs
        .iter()
        .filter(|(from, to)| from != to && from.namespace() == to.namespace())
        .cloned()
        .collect();
    // Closures share subtrees across their roots, so without this the same
    // pair is expanded once per path that reaches it and the queue grows
    // with the product rather than the sum.
    let mut seen = BTreeSet::new();
    while let Some((from, wanted)) = queue.pop_front() {
        if seen.len() >= SLICE_BASE_MAX_OBJECTS || hints.len() >= SLICE_BASE_MAX_OBJECTS {
            break;
        }
        if !seen.insert((from.clone(), wanted.clone())) {
            continue;
        }
        if wanted.namespace().as_str() == crate::BLOB_NAMESPACE {
            hints.entry(wanted).or_insert(from);
            continue;
        }
        let (Some(held_tree), Some(wanted_tree)) = (
            read_held_directory(hold, &from, directory_limit).await?,
            read_held_directory(hold, &wanted, directory_limit).await?,
        ) else {
            continue;
        };
        for (name, node) in wanted_tree.nodes() {
            match (held_tree.get(name), node) {
                (Some(Node::File { digest: old, .. }), Node::File { digest: new, .. })
                    if old == new =>
                {
                    held.insert(ObjectKey::blob(*new));
                }
                (
                    Some(Node::Directory { digest: old, .. }),
                    Node::Directory { digest: new, .. },
                ) if old == new => {
                    held.insert(ObjectKey::directory(*new));
                }
                (Some(Node::File { digest: old, .. }), Node::File { digest: new, .. })
                    if old != new =>
                {
                    hints
                        .entry(ObjectKey::blob(*new))
                        .or_insert_with(|| ObjectKey::blob(*old));
                }
                (
                    Some(Node::Directory { digest: old, .. }),
                    Node::Directory { digest: new, .. },
                ) if old != new => {
                    queue.push_back((ObjectKey::directory(*old), ObjectKey::directory(*new)));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Index the held counterpart of `wanted` before serving it, if one was
/// paired and it is not indexed yet. A full index starts over so the
/// current pair is always covered.
async fn index_slice_hint<PS, SS>(
    hold: &RetentionHold<'_, PS, SS>,
    index: &mut SliceIndex,
    hints: &BTreeMap<ObjectKey, ObjectKey>,
    wanted: &ObjectKey,
) -> Result<(), RepositoryError>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let Some(held) = hints.get(wanted) else {
        return Ok(());
    };
    let Some((record, reader)) = hold.open_payload(held).await? else {
        return Ok(());
    };
    if record.payload_size() == 0 || index.contains(&record.payload()) {
        return Ok(());
    }
    if index.is_full() {
        *index = SliceIndex::new(SLICE_INDEX_MAX_ENTRIES);
    }
    let indexed = index.index_blob(record.payload(), reader).await?;
    tracing::debug!(
        %wanted,
        %held,
        bytes = indexed,
        entries = index.entries(),
        "indexed the held counterpart of a wanted payload"
    );
    Ok(())
}

#[cfg(test)]
#[path = "ssh/sliced_tests.rs"]
mod sliced_tests;

enum ServerPathProof {
    MissingRoot,
    Fallback,
    Proof(PathProof),
}

async fn build_path_proof<PS, SS>(
    hold: &RetentionHold<'_, PS, SS>,
    root_name: &RootName,
    components: &[PathComponent],
) -> Result<ServerPathProof, String>
where
    PS: BlobStore,
    SS: MetadataStore,
{
    let Some(root) = hold
        .snapshot()
        .root(root_name)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(ServerPathProof::MissingRoot);
    };
    if root.namespace().as_str() != crate::DIRECTORY_NAMESPACE {
        return Err(format!(
            "path selection requires a `{}` source root, got `{}`",
            crate::DIRECTORY_NAMESPACE,
            root.namespace()
        ));
    }

    let mut key = root.clone();
    let mut directories = Vec::new();
    let mut proof_bytes = 32usize
        .saturating_add(root_name.as_str().len())
        .saturating_add(root.encode().len())
        .saturating_add(72);
    for index in 0..=components.len() {
        let Some((record, reader)) = hold
            .open_payload(&key)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Err(format!("transfer source is incomplete at {key}"));
        };
        if record.payload_size() > MAX_CONTROL_FRAME as u64 {
            return Ok(ServerPathProof::Fallback);
        }
        let mut payload = Vec::with_capacity(record.payload_size() as usize);
        reader
            .take(record.payload_size().saturating_add(1))
            .read_to_end(&mut payload)
            .await
            .map_err(|error| error.to_string())?;
        if payload.len() as u64 != record.payload_size() {
            return Err(format!(
                "source payload for {key} has {} bytes, record declares {}",
                payload.len(),
                record.payload_size()
            ));
        }
        proof_bytes = proof_bytes
            .saturating_add(record.encode().len())
            .saturating_add(payload.len())
            .saturating_add(16);
        if proof_bytes > MAX_CONTROL_FRAME {
            return Ok(ServerPathProof::Fallback);
        }
        let directory = Directory::decode(&payload).map_err(|error| error.to_string())?;
        directories.push(PathProofEntry { record, payload });

        if index == components.len() {
            break;
        }
        let Some(node) = directory.get(&components[index]) else {
            break;
        };
        if index + 1 == components.len() {
            break;
        }
        match node {
            Node::Directory { digest, .. } => key = ObjectKey::directory(*digest),
            Node::File { .. } | Node::Symlink { .. } => break,
        }
    }

    Ok(ServerPathProof::Proof(PathProof {
        revision: hold.snapshot().revision(),
        root_name: root_name.clone(),
        root,
        directories,
    }))
}

trait OptionalEof<T> {
    fn optional_eof(self) -> Result<Option<T>, std::io::Error>;
}

impl OptionalEof<u8> for Result<u8, std::io::Error> {
    fn optional_eof(self) -> Result<Option<u8>, std::io::Error> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(error) => Err(error),
        }
    }
}

fn decode_key(encoded: &[u8]) -> Result<ObjectKey, String> {
    ObjectKey::decode(encoded).map_err(|error| error.to_string())
}

fn decode_root_name(encoded: &[u8]) -> Result<RootName, String> {
    let name = std::str::from_utf8(encoded).map_err(|error| error.to_string())?;
    RootName::try_from(name).map_err(|error| error.to_string())
}

fn encode_selection(selection: &TransferSelection) -> Result<Vec<u8>, TransferError> {
    selection.validate()?;
    let TransferSelection::Selected { objects, roots } = selection else {
        return Err(TransferError::SourceTransport(
            "snapshot selection needs no command".into(),
        ));
    };
    let keys = encode_object_keys(objects)?;
    let mut body = Vec::new();
    body.extend_from_slice(&(keys.len() as u64).to_le_bytes());
    body.extend_from_slice(&keys);
    body.extend_from_slice(&(roots.len() as u64).to_le_bytes());
    for root in roots {
        body.extend_from_slice(&(root.as_str().len() as u64).to_le_bytes());
        body.extend_from_slice(root.as_str().as_bytes());
    }
    Ok(body)
}

fn take_selection_bytes<'a>(body: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], String> {
    let len = usize::try_from(take_u64(body, cursor)?).map_err(|_| "selection length overflows")?;
    let end = cursor
        .checked_add(len)
        .ok_or("selection length overflows")?;
    let bytes = body.get(*cursor..end).ok_or("truncated selection")?;
    *cursor = end;
    Ok(bytes)
}

fn decode_selection(body: &[u8]) -> Result<TransferSelection, String> {
    if body.len() > MAX_CONTROL_FRAME {
        return Err("selection exceeds frame limit".into());
    }
    let mut cursor = 0;
    let objects = decode_object_keys(take_selection_bytes(body, &mut cursor)?)?;
    let count =
        usize::try_from(take_u64(body, &mut cursor)?).map_err(|_| "selection count overflows")?;
    if count > MAX_TRANSFER_REQUEST_OBJECTS.saturating_sub(objects.len()) {
        return Err("selection has too many entries".into());
    }
    let mut roots = Vec::new();
    for _ in 0..count {
        roots.push(decode_root_name(take_selection_bytes(body, &mut cursor)?)?);
    }
    if cursor != body.len() {
        return Err("trailing selection bytes".into());
    }
    let selection = TransferSelection::Selected { objects, roots };
    selection.validate().map_err(|error| error.to_string())?;
    Ok(selection)
}

fn encode_object_keys(keys: &[ObjectKey]) -> Result<Vec<u8>, TransferError> {
    if keys.len() > MAX_TRANSFER_REQUEST_OBJECTS {
        return Err(TransferError::SourceTransport(format!(
            "object batch has {} keys, limit is {MAX_TRANSFER_REQUEST_OBJECTS}",
            keys.len()
        )));
    }
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&(keys.len() as u64).to_le_bytes());
    for key in keys {
        let key = key.encode();
        encoded.extend_from_slice(&(key.len() as u64).to_le_bytes());
        encoded.extend_from_slice(&key);
        if encoded.len() > MAX_CONTROL_FRAME {
            return Err(TransferError::SourceTransport(format!(
                "object batch exceeds {MAX_CONTROL_FRAME} bytes"
            )));
        }
    }
    Ok(encoded)
}

fn decode_object_keys(encoded: &[u8]) -> Result<Vec<ObjectKey>, String> {
    let mut cursor = 0usize;
    let count = take_u64(encoded, &mut cursor)?;
    let count = usize::try_from(count).map_err(|_| "object batch count overflows".to_owned())?;
    if count > MAX_TRANSFER_REQUEST_OBJECTS {
        return Err(format!(
            "object batch has {count} keys, limit is {MAX_TRANSFER_REQUEST_OBJECTS}"
        ));
    }
    let mut keys = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let length = take_u64(encoded, &mut cursor)?;
        let length = usize::try_from(length)
            .map_err(|_| "object key length overflows this platform".to_owned())?;
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| "object key length overflows this platform".to_owned())?;
        let key = encoded
            .get(cursor..end)
            .ok_or_else(|| "truncated object-key batch".to_owned())?;
        keys.push(decode_key(key)?);
        cursor = end;
    }
    if cursor != encoded.len() {
        return Err("trailing bytes after object-key batch".to_owned());
    }
    Ok(keys)
}

fn take_u64(encoded: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| "object batch length overflows this platform".to_owned())?;
    let bytes: [u8; 8] = encoded
        .get(*cursor..end)
        .ok_or_else(|| "truncated object-key batch".to_owned())?
        .try_into()
        .expect("the exact integer width was checked");
    *cursor = end;
    Ok(u64::from_le_bytes(bytes))
}

async fn write_control(
    output: &mut (impl AsyncWrite + Unpin),
    body: &[u8],
) -> Result<(), std::io::Error> {
    if body.len() > MAX_CONTROL_FRAME {
        return write_error(output, "control response exceeds its limit").await;
    }
    write_response_header(output, STATUS_OK, body.len() as u64).await?;
    output.write_all(body).await?;
    output.flush().await
}

async fn write_absent(output: &mut (impl AsyncWrite + Unpin)) -> Result<(), std::io::Error> {
    write_response_header(output, STATUS_ABSENT, 0).await?;
    output.flush().await
}

async fn write_fallback(output: &mut (impl AsyncWrite + Unpin)) -> Result<(), std::io::Error> {
    write_response_header(output, STATUS_FALLBACK, 0).await?;
    output.flush().await
}

async fn write_error(
    output: &mut (impl AsyncWrite + Unpin),
    message: &str,
) -> Result<(), std::io::Error> {
    let body = message.as_bytes();
    let body = &body[..body.len().min(MAX_ERROR_FRAME)];
    write_response_header(output, STATUS_ERROR, body.len() as u64).await?;
    output.write_all(body).await?;
    output.flush().await
}

async fn write_response_header(
    output: &mut (impl AsyncWrite + Unpin),
    status: u8,
    length: u64,
) -> Result<(), std::io::Error> {
    let mut header = [0u8; RESPONSE_HEADER_LEN];
    header[0] = status;
    header[1..].copy_from_slice(&length.to_le_bytes());
    output.write_all(&header).await
}

/// Server-side SSH stdio adapter failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SshTransportError {
    /// Repository snapshot or payload access failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    /// Stdio failed or the peer disconnected mid-frame.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The bounded source protocol could not be completed safely.
    #[error("SSH transfer protocol failed: {0}")]
    Protocol(String),
}

#[cfg(test)]
mod tests {
    #[test]
    fn selection_frames_reject_truncation_trailing_bytes_and_excessive_counts() {
        let selection = TransferSelection::Selected {
            objects: vec![ObjectKey::blob(crate::BlobId::new(crate::Digest::from(
                [7; 32],
            )))],
            roots: vec![RootName::try_from("root").unwrap()],
        };
        let body = encode_selection(&selection).unwrap();
        assert_eq!(decode_selection(&body).unwrap(), selection);
        for length in 0..body.len() {
            assert!(decode_selection(&body[..length]).is_err());
        }
        let mut trailing = body.clone();
        trailing.push(0);
        assert!(decode_selection(&trailing).is_err());
        let mut count = encode_selection(&TransferSelection::Selected {
            objects: Vec::new(),
            roots: Vec::new(),
        })
        .unwrap();
        count[16..24].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode_selection(&count).is_err());
        assert!(
            encode_selection(&TransferSelection::Selected {
                objects: Vec::new(),
                roots: vec![RootName::try_from("root").unwrap(); MAX_TRANSFER_REQUEST_OBJECTS + 1],
            })
            .is_err()
        );
    }

    #[tokio::test]
    async fn selection_is_acknowledged_once_and_cannot_expand_after_reads() {
        for select_first in [false, true] {
            let source = Repository::memory().unwrap();
            let (client, server) = tokio::io::duplex(4096);
            let (input, output) = tokio::io::split(server);
            let server =
                tokio::spawn(async move { serve_transfer_stdio(&source, input, output).await });
            let (input, output) = tokio::io::split(client);
            let session = SshTransferSession::connect(Box::new(input), Box::new(output), None)
                .await
                .unwrap();
            let selected = TransferSelection::Selected {
                objects: Vec::new(),
                roots: Vec::new(),
            };
            if select_first {
                session.select(&selected).await.unwrap();
            } else {
                assert!(
                    session
                        .root(&RootName::try_from("absent").unwrap())
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            assert!(session.select(&selected).await.is_err());
            drop(session);
            server.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn peer_must_acknowledge_selection_before_session_is_exposed() {
        let (client, mut server) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            server.write_all(PROTOCOL_MAGIC).await.unwrap();
            server
                .write_all(RepositoryRevision::from_bytes([4; 32]).as_bytes())
                .await
                .unwrap();
            server.flush().await.unwrap();
            assert_eq!(server.read_u8().await.unwrap(), OP_SELECTION);
            let length = server.read_u64_le().await.unwrap();
            let mut body = vec![0; length as usize];
            server.read_exact(&mut body).await.unwrap();
            decode_selection(&body).unwrap();
            write_absent(&mut server).await.unwrap();
        });
        let (input, output) = tokio::io::split(client);
        assert!(
            connect_transfer_stdio_source(
                input,
                output,
                TransferSelection::Selected {
                    objects: Vec::new(),
                    roots: Vec::new(),
                }
            )
            .await
            .is_err()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bao_streams_cross_fragmented_transport_and_reuse_connection() {
        use super::*;
        use crate::sync::TransferReadSession;
        for size in [0usize, 1, 16383, 16384, 16385, 131073] {
            let source = Repository::memory().unwrap();
            let bytes = (0..size).map(|i| (i * 17) as u8).collect::<Vec<_>>();
            let mutation = source.mutation_session().await.unwrap();
            let staged = mutation.stage_blob(&bytes).await.unwrap();
            let record = staged.record().clone();
            let name = RootName::try_from("file").unwrap();
            mutation
                .publish_rooted(vec![staged], name.clone(), record.key().clone())
                .await
                .unwrap();
            drop(mutation);
            let (client, server) = tokio::io::duplex(257);
            let (client_read, client_write) = tokio::io::split(client);
            let (server_read, server_write) = tokio::io::split(server);
            let server = tokio::spawn(async move {
                serve_transfer_stdio(&source, server_read, server_write).await
            });
            let session =
                SshTransferSession::connect(Box::new(client_read), Box::new(client_write), None)
                    .await
                    .unwrap();
            let mut reader = session.open_verified(&record).await.unwrap().unwrap();
            let mut output = Vec::new();
            reader.read_to_end(&mut output).await.unwrap();
            assert_eq!(output, bytes);
            drop(reader);
            assert_eq!(
                session.root(&name).await.unwrap(),
                Some(record.key().clone())
            );
            drop(session);
            server.await.unwrap().unwrap();
        }
    }

    use super::*;
    use crate::test_util::pc;
    use crate::{
        BlobId, DestinationRoot, Digest, Directory, MemoryBlobStore, MemoryMetadataStore, Node,
        ObjectRequest, RootChange,
    };
    use std::ffi::OsStr;

    #[tokio::test]
    async fn invalid_or_cancelled_payload_batches_poison_the_connection() {
        let source = Repository::memory().unwrap();
        let mutation = source.mutation_session().await.unwrap();
        let staged = mutation.stage_blob(b"payload").await.unwrap();
        let records = [staged.record().clone()];
        // Wrong count, oversized entry, and a truncated body must all fail
        // without allowing a subsequent request to consume residual frames.
        for (count, length, body) in [
            (2, 7, b"payload".as_slice()),
            (1, 8, b"payload"),
            (1, 7, b"pay"),
        ] {
            let mut response = Vec::new();
            response.push(STATUS_OK);
            response.extend_from_slice(&u64::to_le_bytes(count));
            response.push(STATUS_OK);
            response.extend_from_slice(&u64::to_le_bytes(length));
            response.extend_from_slice(body);
            let session = test_batch_session(Box::new(std::io::Cursor::new(response)));
            assert!(
                session
                    .payload_batch(&records, 1024, &NoSources)
                    .await
                    .is_err()
            );
            assert!(session.object(records[0].key()).await.is_err());
        }
        let (reader, _peer) = tokio::io::duplex(64);
        let session = test_batch_session(Box::new(reader));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                session.payload_batch(&records, 1024, &NoSources)
            )
            .await
            .is_err()
        );
        assert!(session.object(records[0].key()).await.is_err());
    }

    fn test_batch_session(reader: Box<dyn AsyncRead + Send + Unpin>) -> SshTransferSession {
        SshTransferSession {
            revision: RepositoryRevision::from_bytes([0; 32]),
            connection: Arc::new(RpcConnection::new(
                reader,
                Box::new(tokio::io::sink()),
                None,
            )),
        }
    }

    #[tokio::test]
    async fn payload_batches_preserve_order_and_enforce_limits() {
        let source = Repository::memory().unwrap();
        let mut records = Vec::new();
        for bytes in [b"first".as_slice(), b"second".as_slice()] {
            let mutation = source.mutation_session().await.unwrap();
            let staged = mutation.stage_blob(bytes).await.unwrap();
            records.push(staged.record().clone());
            mutation.publish_unrooted(vec![staged]).await.unwrap();
        }
        let (client, server) = tokio::io::duplex(64);
        let (client_read, client_write) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let task =
            tokio::spawn(
                async move { serve_transfer_stdio(&source, server_read, server_write).await },
            );
        let session =
            SshTransferSession::connect(Box::new(client_read), Box::new(client_write), None)
                .await
                .unwrap();
        assert!(
            session
                .payload_batch(&records, 1, &NoSources)
                .await
                .unwrap()
                .is_none()
        );
        let ordered = [records[1].clone(), records[0].clone(), records[1].clone()];
        let readers = session
            .payload_batch(&ordered, 1024, &NoSources)
            .await
            .unwrap()
            .unwrap();
        for (payload, expected) in readers.into_iter().zip([
            b"second".as_slice(),
            b"first".as_slice(),
            b"second".as_slice(),
        ]) {
            let (mut reader, _) = payload.unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
        }
        assert!(session.object(records[0].key()).await.unwrap().is_some());
        let keys = ordered
            .iter()
            .map(|record| record.key().clone())
            .collect::<Vec<_>>();
        let requests = session.transport_requests();
        assert!(
            session
                .object_payload_batch(&keys, 1024, &NoSources)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(session.transport_requests(), requests);
        let batch = session
            .object_payload_batch(&keys, MAX_PAYLOAD_BATCH_BYTES, &NoSources)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            batch.records,
            ordered.into_iter().map(Some).collect::<Vec<_>>()
        );
        for (payload, expected) in
            batch
                .payloads
                .unwrap()
                .into_iter()
                .zip([b"second".as_slice(), b"first", b"second"])
        {
            let (mut reader, _) = payload.unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
        }
        let missing = ObjectKey::blob(BlobId::new(Digest::hash(b"absent")));
        let batch = session
            .object_payload_batch(&[missing], MAX_PAYLOAD_BATCH_BYTES, &NoSources)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.records, vec![None]);
        assert!(batch.payloads.is_none());
        assert!(session.object(records[0].key()).await.unwrap().is_some());
        drop(session);
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn pipelined_batches_reject_bad_frames_and_poison_on_cancellation() {
        let source = Repository::memory().unwrap();
        let mutation = source.mutation_session().await.unwrap();
        let staged = mutation.stage_blob(b"payload").await.unwrap();
        let record = staged.record().clone();
        let keys = [record.key().clone()];
        let encoded = crate::sync::wire::encode_records(&[record]);
        for (count, length, body) in [
            (2u64, 7u64, b"payload".as_slice()),
            (1, 8, b"payload"),
            (1, 7, b"pay"),
        ] {
            let mut response = Vec::new();
            response.push(STATUS_OK);
            response.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
            response.extend_from_slice(&encoded);
            response.push(STATUS_OK);
            response.extend_from_slice(&count.to_le_bytes());
            response.push(STATUS_OK);
            response.extend_from_slice(&length.to_le_bytes());
            response.extend_from_slice(body);
            let session = test_batch_session(Box::new(std::io::Cursor::new(response)));
            assert!(
                session
                    .object_payload_batch(&keys, MAX_PAYLOAD_BATCH_BYTES, &NoSources)
                    .await
                    .is_err()
            );
            assert!(session.object(&keys[0]).await.is_err());
        }
        let (reader, _peer) = tokio::io::duplex(64);
        let session = test_batch_session(Box::new(reader));
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(1),
                session.object_payload_batch(&keys, MAX_PAYLOAD_BATCH_BYTES, &NoSources)
            )
            .await
            .is_err()
        );
        assert!(session.object(&keys[0]).await.is_err());
    }

    #[test]
    fn endpoint_parser_and_ssh_arguments_are_injection_safe() {
        let endpoint: SshEndpoint = "ssh://alice@example.com:2222/var/lib/casita%20source"
            .parse()
            .unwrap();
        assert_eq!(endpoint.destination(), "alice@example.com");
        assert_eq!(endpoint.port(), Some(2222));
        assert_eq!(endpoint.repository(), Path::new("/var/lib/casita source"));

        let source = SshTransferSource::new(endpoint);
        let arguments = source.arguments();
        // Options are complete `-o` pairs ahead of the destination, so no
        // endpoint text can land in an option position.
        let (options, command) = arguments.split_at(arguments.len() - 5);
        let expected = "-T -o ClearAllForwardings=yes -o ConnectTimeout=30 \
                        -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -p 2222";
        assert_eq!(
            options,
            expected
                .split_whitespace()
                .map(OsString::from)
                .collect::<Vec<_>>()
        );
        assert_eq!(command[0], OsStr::new("alice@example.com"));
        assert_eq!(command[1], OsStr::new("casita"));
        assert_eq!(command[2], OsStr::new("__ssh-source"));
        assert!(
            command[4]
                .to_string_lossy()
                .bytes()
                .all(|byte| { byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_') })
        );

        for invalid in [
            "https://host/repo",
            "ssh://-oProxyCommand=bad/repo",
            "ssh://host/relative?query",
            "ssh://user;bad@host/repo",
            "ssh://host:0/repo",
            "ssh://host/%zz",
        ] {
            assert!(
                invalid.parse::<SshEndpoint>().is_err(),
                "accepted {invalid}"
            );
        }
    }

    #[tokio::test]
    async fn ssh_peer_can_request_bounded_path_spine_fallback() {
        let (client, mut server) = tokio::io::duplex(4096);
        let remote = tokio::spawn(async move {
            server.write_all(PROTOCOL_MAGIC).await.unwrap();
            server
                .write_all(RepositoryRevision::from_bytes([5; 32]).as_bytes())
                .await
                .unwrap();
            server.flush().await.unwrap();
            assert_eq!(server.read_u8().await.unwrap(), OP_PATH_PROOF);
            let length = server.read_u64_le().await.unwrap();
            let mut request = vec![0; usize::try_from(length).unwrap()];
            server.read_exact(&mut request).await.unwrap();
            let (root, components) =
                crate::sync::wire::decode_path_proof_request(&request).unwrap();
            assert_eq!(root.as_str(), "releases/current");
            assert_eq!(components, [pc("sub")]);
            write_fallback(&mut server).await.unwrap();
        });
        let (client_read, client_write) = tokio::io::split(client);
        let session =
            SshTransferSession::connect(Box::new(client_read), Box::new(client_write), None)
                .await
                .unwrap();
        assert_eq!(
            session
                .path_proof(
                    &RootName::try_from("releases/current").unwrap(),
                    &[pc("sub")],
                )
                .await
                .unwrap(),
            PathProofResponse::Unsupported
        );
        drop(session);
        remote.await.unwrap();
    }

    #[tokio::test]
    async fn split_source_works_with_remote_metadata_or_remote_payloads() {
        for remote_blobs in [false, true] {
            let original =
                Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
            let name = RootName::try_from("main").unwrap();
            let mutation = original.mutation_session().await.unwrap();
            let staged = mutation.stage_blob(b"split remote payload").await.unwrap();
            let root = staged.record().key().clone();
            mutation
                .publish(
                    vec![staged],
                    vec![RootChange::Set {
                        name: name.clone(),
                        target: root.clone(),
                    }],
                )
                .await
                .unwrap();
            drop(mutation);
            let metadata = Repository::new(MemoryBlobStore::new(), original.metadata().clone());
            let (remote, local) = if remote_blobs {
                (original, metadata)
            } else {
                (metadata, original)
            };
            let (client, server) = tokio::io::duplex(16 * 1024);
            let (client_read, client_write) = tokio::io::split(client);
            let (server_read, server_write) = tokio::io::split(server);
            let server = tokio::spawn(async move {
                serve_transfer_stdio(&remote, server_read, server_write).await
            });
            let remote = Box::new(
                SshTransferSession::connect(Box::new(client_read), Box::new(client_write), None)
                    .await
                    .unwrap(),
            );
            let local = local
                .begin_transfer(crate::sync::TransferSelection::Snapshot)
                .await
                .unwrap();
            let session = if remote_blobs {
                crate::sync::SplitTransferSession::new(local, remote)
            } else {
                crate::sync::SplitTransferSession::new(remote, local)
            };
            assert_eq!(session.root(&name).await.unwrap(), Some(root.clone()));
            let destination =
                Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
            let result = crate::transfer(
                &crate::sync::HeldSession(&session),
                &destination,
                crate::TransferRequest {
                    objects: vec![ObjectRequest {
                        key: root.clone(),
                        recursive: true,
                    }],
                    roots: vec![DestinationRoot {
                        name,
                        target: root.clone(),
                    }],
                },
                crate::sync::TransferOptions::default(),
            )
            .await
            .unwrap();
            assert_eq!(result.progress.published_objects, 1);
            assert!(matches!(
                destination.verify_closure(&root).await.unwrap(),
                crate::ClosureStatus::Complete { .. }
            ));
            drop(session);
            server.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn stdio_source_syncs_a_named_root_through_receiver_verification() {
        let source = Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let root_name = RootName::try_from("releases/current").unwrap();
        let mutation = source.mutation_session().await.unwrap();
        let staged = mutation
            .stage_blob(b"authenticated transport")
            .await
            .unwrap();
        let root = staged.record().key().clone();
        mutation
            .publish(
                vec![staged],
                vec![RootChange::Set {
                    name: root_name.clone(),
                    target: root.clone(),
                }],
            )
            .await
            .unwrap();
        drop(mutation);

        let (client, server) = tokio::io::duplex(16 * 1024);
        let (client_read, client_write) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let server =
            tokio::spawn(
                async move { serve_transfer_stdio(&source, server_read, server_write).await },
            );
        let session =
            SshTransferSession::connect(Box::new(client_read), Box::new(client_write), None)
                .await
                .unwrap();
        assert_eq!(session.root(&root_name).await.unwrap(), Some(root.clone()));
        let missing = ObjectKey::blob(BlobId::new(Digest::hash(b"missing")));
        let batch = session
            .objects(&[missing.clone(), root.clone(), missing])
            .await
            .unwrap();
        assert!(batch[0].is_none());
        assert_eq!(batch[1].as_ref().map(ObjectRecord::key), Some(&root));
        assert!(batch[2].is_none());

        let destination =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let result = crate::transfer(
            &crate::sync::HeldSession(&session),
            &destination,
            crate::TransferRequest {
                objects: vec![ObjectRequest {
                    key: root.clone(),
                    recursive: true,
                }],
                roots: vec![DestinationRoot {
                    name: root_name.clone(),
                    target: root.clone(),
                }],
            },
            crate::sync::TransferOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(result.progress.published_objects, 1);
        assert_eq!(result.progress.payloads_sent, 1);
        assert_eq!(
            destination
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .root(&root_name)
                .await
                .unwrap(),
            Some(root.clone())
        );
        assert!(matches!(
            destination.verify_closure(&root).await.unwrap(),
            crate::ClosureStatus::Complete { .. }
        ));

        // Both a complete logical object and an unregistered but present
        // payload must avoid speculative network downloads.
        let payload_only = Repository::memory().unwrap();
        payload_only
            .payloads()
            .put_slice(b"authenticated transport")
            .await
            .unwrap();
        for existing in [&destination, &payload_only] {
            let before = session.transport_requests().unwrap();
            let result = crate::transfer(
                &crate::sync::HeldSession(&session),
                existing,
                crate::TransferRequest {
                    objects: vec![ObjectRequest {
                        key: root.clone(),
                        recursive: true,
                    }],
                    roots: Vec::new(),
                },
                crate::sync::TransferOptions::default(),
            )
            .await
            .unwrap();
            assert_eq!(result.progress.payloads_sent, 0);
            assert_eq!(session.transport_requests().unwrap() - before, 1);
        }

        drop(session);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stdio_source_supports_verified_path_selected_transfer() {
        let source = Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let source_name = RootName::try_from("releases/current").unwrap();
        let mutation = source.mutation_session().await.unwrap();
        let selected = mutation.stage_blob(b"remote path").await.unwrap();
        let selected_key = selected.record().key().clone();
        let sibling = mutation.stage_blob(b"remote sibling").await.unwrap();
        let sibling_key = sibling.record().key().clone();
        let sub = Directory::try_from_iter([
            (
                pc("selected.txt"),
                Node::File {
                    digest: BlobId::new(Digest::hash(b"remote path")),
                    size: b"remote path".len() as u64,
                    executable: false,
                },
            ),
            (
                pc("sibling.txt"),
                Node::File {
                    digest: BlobId::new(Digest::hash(b"remote sibling")),
                    size: b"remote sibling".len() as u64,
                    executable: false,
                },
            ),
        ])
        .unwrap();
        let sub_object = mutation.stage_directory(&sub).await.unwrap();
        let sub_key = sub_object.record().key().clone();
        let root = Directory::try_from_iter([(
            pc("sub"),
            Node::Directory {
                digest: sub.digest(),
                size: sub.size(),
            },
        )])
        .unwrap();
        let root_object = mutation.stage_directory(&root).await.unwrap();
        let root_key = root_object.record().key().clone();
        mutation
            .publish(
                vec![selected, sibling, sub_object, root_object],
                vec![RootChange::Set {
                    name: source_name.clone(),
                    target: root_key.clone(),
                }],
            )
            .await
            .unwrap();
        drop(mutation);

        let (client, server) = tokio::io::duplex(16 * 1024);
        let (client_read, client_write) = tokio::io::split(client);
        let (server_read, server_write) = tokio::io::split(server);
        let server =
            tokio::spawn(
                async move { serve_transfer_stdio(&source, server_read, server_write).await },
            );
        let session =
            SshTransferSession::connect(Box::new(client_read), Box::new(client_write), None)
                .await
                .unwrap();
        let proof = session
            .path_proof(&source_name, &[pc("sub"), pc("selected.txt")])
            .await
            .unwrap();
        let PathProofResponse::Proof(proof) = proof else {
            panic!("capable server did not return a path proof");
        };
        assert_eq!(proof.revision, session.revision());
        assert_eq!(proof.root, root_key);
        assert_eq!(proof.directories.len(), 2);
        assert_eq!(
            session
                .path_proof(
                    &RootName::try_from("missing/root").unwrap(),
                    &[pc("sub"), pc("selected.txt")],
                )
                .await
                .unwrap(),
            PathProofResponse::MissingRoot
        );
        let destination =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let destination_name = RootName::try_from("partial/remote-path").unwrap();
        let outcome = crate::transfer_path(
            &crate::sync::HeldSession(&session),
            &destination,
            &source_name,
            "sub/selected.txt",
            Some(destination_name.clone()),
            crate::sync::TransferOptions::default(),
        )
        .await
        .unwrap();
        assert!(matches!(outcome.node, Some(Node::File { .. })));
        assert_eq!(outcome.transfer.unwrap().progress.published_objects, 1);
        let snapshot = destination.metadata().snapshot().await.unwrap();
        assert_eq!(
            snapshot.root(&destination_name).await.unwrap(),
            Some(selected_key.clone())
        );
        assert!(snapshot.object(&selected_key).await.unwrap().is_some());
        assert!(snapshot.object(&root_key).await.unwrap().is_none());
        assert!(snapshot.object(&sub_key).await.unwrap().is_none());
        assert!(snapshot.object(&sibling_key).await.unwrap().is_none());

        drop(session);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn receiver_rejects_payload_substitution_from_remote_source() {
        let expected = b"expected payload";
        let substituted = b"substituted payload";
        let key = ObjectKey::blob(BlobId::new(Digest::hash(expected)));
        let substituted_payload = BlobId::new(Digest::hash(substituted));
        let advertised = ObjectRecord::new(
            key.clone(),
            substituted_payload,
            substituted.len() as u64,
            Vec::new(),
        )
        .unwrap();

        let (client, mut server) = tokio::io::duplex(16 * 1024);
        let expected_key = key.clone();
        let remote = tokio::spawn(async move {
            // A lying source answers every request for the object with a record
            // and bytes for a different payload. Batched paths it may decline,
            // as an honest source can, so the client falls back to single reads.
            server.write_all(PROTOCOL_MAGIC).await.unwrap();
            server
                .write_all(RepositoryRevision::from_bytes([7; 32]).as_bytes())
                .await
                .unwrap();
            server.flush().await.unwrap();

            let mut payloads_sent = 0;
            while let Ok(operation) = server.read_u8().await {
                let length = server.read_u64_le().await.unwrap();
                let mut body = vec![0u8; usize::try_from(length).unwrap()];
                server.read_exact(&mut body).await.unwrap();
                match operation {
                    OP_DISCOVER | OP_PAYLOADS => write_fallback(&mut server).await.unwrap(),
                    OP_OBJECTS => {
                        assert_eq!(
                            decode_object_keys(&body).unwrap(),
                            std::slice::from_ref(&expected_key)
                        );
                        write_control(
                            &mut server,
                            &crate::sync::wire::encode_records(std::slice::from_ref(&advertised)),
                        )
                        .await
                        .unwrap();
                    }
                    OP_OBJECT => {
                        assert_eq!(body, expected_key.encode());
                        write_control(&mut server, &advertised.encode())
                            .await
                            .unwrap();
                    }
                    OP_PAYLOAD | OP_SLICED => {
                        assert_eq!(body, expected_key.encode());
                        payloads_sent += 1;
                        write_response_header(&mut server, STATUS_OK, substituted.len() as u64)
                            .await
                            .unwrap();
                        encode_literal_stream(
                            substituted_payload,
                            substituted.len() as u64,
                            std::io::Cursor::new(substituted),
                            &mut server,
                        )
                        .await
                        .unwrap();
                        server.flush().await.unwrap();
                    }
                    other => panic!("unexpected request {other}"),
                }
            }
            assert!(payloads_sent > 0, "the forged payload was never requested");
        });

        let (client_read, client_write) = tokio::io::split(client);
        let session =
            SshTransferSession::connect(Box::new(client_read), Box::new(client_write), None)
                .await
                .unwrap();
        let destination =
            Repository::new(MemoryBlobStore::new(), MemoryMetadataStore::new().unwrap());
        let error = crate::transfer(
            &crate::sync::HeldSession(&session),
            &destination,
            crate::TransferRequest {
                objects: vec![ObjectRequest {
                    key: key.clone(),
                    recursive: true,
                }],
                roots: Vec::new(),
            },
            crate::sync::TransferOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            TransferError::Destination(RepositoryError::Format(_))
        ));
        assert!(
            destination
                .metadata()
                .snapshot()
                .await
                .unwrap()
                .object(&key)
                .await
                .unwrap()
                .is_none()
        );

        drop(session);
        remote.await.unwrap();
    }
}
