//! Generic logical object identities and records.
//!
//! Payload bytes retain their content-only [`BlobId`] identity. A logical
//! object adds a versioned [`NamespaceId`] and that format's canonical native
//! identifier. Repository algorithms operate on [`ObjectKey`] and the verified
//! forward links in [`ObjectRecord`] without understanding the object format.

use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use data_encoding::BASE64URL_NOPAD;

use crate::{BlobId, Digest, DirectoryId};

/// Maximum encoded length of a namespace identifier.
pub const MAX_NAMESPACE_LEN: usize = 64;
/// Maximum encoded length of a format-native identifier.
pub const MAX_NATIVE_ID_LEN: usize = 128;
/// Maximum encoded length of a root name.
pub const MAX_ROOT_NAME_LEN: usize = 1024;
/// Maximum encoded length of one root-name segment.
pub const MAX_ROOT_SEGMENT_LEN: usize = 255;

/// Frozen namespace of unstructured BLAKE3-addressed payloads.
pub const BLOB_NAMESPACE: &str = "casita.blob.v1";
/// Frozen namespace of canonical filesystem-directory payloads.
pub const DIRECTORY_NAMESPACE: &str = "casita.directory.v1";

/// A validated, versioned object-format namespace.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceId(String);

/// Why a namespace identifier is invalid.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum NamespaceIdError {
    /// Namespace identifiers are never empty.
    #[error("namespace identifier is empty")]
    Empty,
    /// The identifier exceeds the durable-format ceiling.
    #[error("namespace identifier exceeds {MAX_NAMESPACE_LEN} bytes")]
    TooLong,
    /// Only ASCII is permitted.
    #[error("namespace identifier is not ASCII")]
    NonAscii,
    /// The identifier does not match `label(.label)*.v<digits>`.
    #[error("namespace identifier must match `label(.label)*.v<digits>`")]
    InvalidSyntax,
}

impl NamespaceId {
    fn validate(value: &str) -> Result<(), NamespaceIdError> {
        if value.is_empty() {
            return Err(NamespaceIdError::Empty);
        }
        if value.len() > MAX_NAMESPACE_LEN {
            return Err(NamespaceIdError::TooLong);
        }
        if !value.is_ascii() {
            return Err(NamespaceIdError::NonAscii);
        }

        let mut segments = value.split('.').peekable();
        let mut ordinary = 0usize;
        while let Some(segment) = segments.next() {
            if segments.peek().is_none() {
                let Some(digits) = segment.strip_prefix('v') else {
                    return Err(NamespaceIdError::InvalidSyntax);
                };
                if ordinary == 0
                    || digits.is_empty()
                    || !digits.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(NamespaceIdError::InvalidSyntax);
                }
                return Ok(());
            }

            ordinary += 1;
            let mut bytes = segment.bytes();
            if !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
                || !bytes
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            {
                return Err(NamespaceIdError::InvalidSyntax);
            }
        }

        Err(NamespaceIdError::InvalidSyntax)
    }

    /// The exact ASCII namespace bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    /// The namespace string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for NamespaceId {
    type Error = NamespaceIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::validate(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for NamespaceId {
    type Error = NamespaceIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::validate(&value)?;
        Ok(Self(value))
    }
}

impl FromStr for NamespaceId {
    type Err = NamespaceIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for NamespaceId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("NamespaceId").field(&self.as_str()).finish()
    }
}

/// A namespace-qualified native object identity.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    namespace: NamespaceId,
    native_id: Bytes,
}

/// Why an object key is invalid.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ObjectKeyError {
    /// The namespace is malformed.
    #[error(transparent)]
    Namespace(#[from] NamespaceIdError),
    /// The format-native identifier is too large.
    #[error("native object identifier exceeds {MAX_NATIVE_ID_LEN} bytes")]
    NativeIdTooLong,
    /// The text form must contain a namespace and native identifier separated
    /// by one colon.
    #[error("object key text must be `<namespace>:<base64url-native-id>`")]
    InvalidText,
    /// The native identifier is not canonical unpadded URL-safe base64.
    #[error("invalid object-key base64: {0}")]
    InvalidBase64(String),
}

impl ObjectKey {
    /// Construct a key from an already validated namespace and the format's
    /// canonical native identifier.
    pub fn new(
        namespace: NamespaceId,
        native_id: impl Into<Bytes>,
    ) -> Result<Self, ObjectKeyError> {
        let native_id = native_id.into();
        if native_id.len() > MAX_NATIVE_ID_LEN {
            return Err(ObjectKeyError::NativeIdTooLong);
        }
        Ok(Self {
            namespace,
            native_id,
        })
    }

    /// The namespace selecting this key's exact semantics.
    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    /// The canonical opaque identifier within the namespace.
    pub fn native_id(&self) -> &[u8] {
        &self.native_id
    }

    /// A key in the frozen raw-blob namespace.
    pub fn blob(id: BlobId) -> Self {
        Self {
            namespace: NamespaceId(BLOB_NAMESPACE.to_owned()),
            native_id: Bytes::copy_from_slice(id.digest().as_bytes()),
        }
    }

    /// A key in the frozen canonical-directory namespace.
    pub fn directory(id: DirectoryId) -> Self {
        Self {
            namespace: NamespaceId(DIRECTORY_NAMESPACE.to_owned()),
            native_id: Bytes::copy_from_slice(id.digest().as_bytes()),
        }
    }

    /// Return the native identifier as a digest when it has the right width.
    pub fn native_digest(&self) -> Option<Digest> {
        Digest::try_from(self.native_id()).ok()
    }

    /// Encode the frozen object-key layout.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(16 + self.namespace.0.len() + self.native_id.len());
        encode_bytes(&mut encoded, self.namespace.as_bytes());
        encode_bytes(&mut encoded, &self.native_id);
        encoded
    }

    /// Decode exactly one frozen object-key encoding.
    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, LogicalEncodingError> {
        let mut reader = EncodingReader::new(encoded);
        let key = Self::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(key)
    }

    fn encode_into(&self, encoded: &mut Vec<u8>) {
        encode_bytes(encoded, self.namespace.as_bytes());
        encode_bytes(encoded, &self.native_id);
    }

    fn decode_from(reader: &mut EncodingReader<'_>) -> Result<Self, LogicalEncodingError> {
        let namespace = reader.read_bounded_bytes(MAX_NAMESPACE_LEN)?;
        let namespace = std::str::from_utf8(namespace)
            .map_err(|_| LogicalEncodingError::Namespace(NamespaceIdError::NonAscii))?;
        let namespace = NamespaceId::try_from(namespace)?;
        let native_id = Bytes::copy_from_slice(reader.read_bounded_bytes(MAX_NATIVE_ID_LEN)?);
        Ok(Self::new(namespace, native_id)?)
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}",
            self.namespace,
            BASE64URL_NOPAD.encode(&self.native_id)
        )
    }
}

impl fmt::Debug for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ObjectKey").field(&self.to_string()).finish()
    }
}

impl FromStr for ObjectKey {
    type Err = ObjectKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (namespace, native_id) = value.split_once(':').ok_or(ObjectKeyError::InvalidText)?;
        if native_id.contains(':') {
            return Err(ObjectKeyError::InvalidText);
        }
        let namespace = NamespaceId::try_from(namespace)?;
        let native_id = BASE64URL_NOPAD
            .decode(native_id.as_bytes())
            .map_err(|error| ObjectKeyError::InvalidBase64(error.to_string()))?;
        Self::new(namespace, native_id)
    }
}

/// A canonical durable name retaining one object's complete forward closure.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RootName(String);

/// Why a root name is invalid.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RootNameError {
    /// Root names have at least one segment.
    #[error("root name is empty")]
    Empty,
    /// The complete UTF-8 name is too large.
    #[error("root name exceeds {MAX_ROOT_NAME_LEN} bytes")]
    TooLong,
    /// A leading, trailing, or doubled slash produced an empty segment.
    #[error("root name contains an empty segment")]
    EmptySegment,
    /// A segment is `.` or `..`.
    #[error("root name contains a `.` or `..` segment")]
    DotSegment,
    /// One segment exceeds the durable-format ceiling.
    #[error("root name segment exceeds {MAX_ROOT_SEGMENT_LEN} bytes")]
    SegmentTooLong,
    /// C0 controls and DEL are forbidden.
    #[error("root name contains an ASCII control character")]
    Control,
}

impl RootName {
    fn validate(value: &str) -> Result<(), RootNameError> {
        if value.is_empty() {
            return Err(RootNameError::Empty);
        }
        if value.len() > MAX_ROOT_NAME_LEN {
            return Err(RootNameError::TooLong);
        }
        for segment in value.split('/') {
            if segment.is_empty() {
                return Err(RootNameError::EmptySegment);
            }
            if segment == "." || segment == ".." {
                return Err(RootNameError::DotSegment);
            }
            if segment.len() > MAX_ROOT_SEGMENT_LEN {
                return Err(RootNameError::SegmentTooLong);
            }
            if segment.bytes().any(|byte| byte <= 0x1f || byte == 0x7f) {
                return Err(RootNameError::Control);
            }
        }
        Ok(())
    }

    /// The exact UTF-8 root name.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this name is equal to or below `prefix` on segment boundaries.
    pub fn is_under(&self, prefix: &RootName) -> bool {
        self == prefix
            || self
                .as_str()
                .strip_prefix(prefix.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'))
    }
}

impl TryFrom<&str> for RootName {
    type Error = RootNameError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::validate(value)?;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<String> for RootName {
    type Error = RootNameError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::validate(&value)?;
        Ok(Self(value))
    }
}

impl FromStr for RootName {
    type Err = RootNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::try_from(value)
    }
}

impl AsRef<str> for RootName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for RootName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for RootName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RootName").field(&self.as_str()).finish()
    }
}

/// The durable encoding of one named-root mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootRecord {
    name: RootName,
    target: ObjectKey,
}

impl RootRecord {
    /// Construct a root record from validated parts.
    pub(crate) fn new(name: RootName, target: ObjectKey) -> Self {
        Self { name, target }
    }

    /// The canonical name.
    pub fn name(&self) -> &RootName {
        &self.name
    }

    /// The exact retained object.
    pub fn target(&self) -> &ObjectKey {
        &self.target
    }

    /// Encode the frozen root-record layout.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        encode_bytes(&mut encoded, self.name.as_str().as_bytes());
        self.target.encode_into(&mut encoded);
        encoded
    }

    /// Decode exactly one frozen root-record encoding.
    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, LogicalEncodingError> {
        let mut reader = EncodingReader::new(encoded);
        let name = reader.read_bounded_bytes(MAX_ROOT_NAME_LEN)?;
        let name = std::str::from_utf8(name).map_err(|_| LogicalEncodingError::InvalidUtf8)?;
        let name = RootName::try_from(name)?;
        let target = ObjectKey::decode_from(&mut reader)?;
        reader.finish()?;
        Ok(Self { name, target })
    }
}

/// An immutable logical record accepted by a namespace verifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRecord {
    key: ObjectKey,
    payload: BlobId,
    payload_size: u64,
    links: Arc<[ObjectKey]>,
}

/// Why an object record is structurally invalid.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ObjectRecordError {
    /// Links must be strictly sorted by object key, which also excludes
    /// duplicates.
    #[error("object-record links are not in canonical strictly ascending order")]
    NonCanonicalLinks,
}

impl ObjectRecord {
    /// Construct the structural representation of a record.
    ///
    /// This validates canonical link ordering but does not verify the payload,
    /// native identity, or claimed links. Repository mutation accepts the
    /// sealed result of an experimental `ObjectFormat` verifier, not
    /// an arbitrary value constructed here.
    pub(crate) fn new(
        key: ObjectKey,
        payload: BlobId,
        payload_size: u64,
        links: Vec<ObjectKey>,
    ) -> Result<Self, ObjectRecordError> {
        if links.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(ObjectRecordError::NonCanonicalLinks);
        }
        Ok(Self {
            key,
            payload,
            payload_size,
            links: links.into(),
        })
    }

    /// The logical identity.
    pub fn key(&self) -> &ObjectKey {
        &self.key
    }

    /// The content-only identity of the complete plaintext payload.
    pub fn payload(&self) -> BlobId {
        self.payload
    }

    /// The verified complete payload length.
    pub fn payload_size(&self) -> u64 {
        self.payload_size
    }

    /// Canonical, sorted, duplicate-free forward links.
    pub fn links(&self) -> &[ObjectKey] {
        &self.links
    }

    /// Encode the frozen v0.2 object-record wire form.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        self.key.encode_into(&mut encoded);
        encoded.extend_from_slice(self.payload.digest().as_bytes());
        encoded.extend_from_slice(&self.payload_size.to_le_bytes());
        encoded.extend_from_slice(&(self.links.len() as u64).to_le_bytes());
        for link in self.links.iter() {
            link.encode_into(&mut encoded);
        }
        encoded
    }

    /// Decode exactly one frozen v0.2 object-record wire form.
    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, LogicalEncodingError> {
        let mut reader = EncodingReader::new(encoded);
        let key = ObjectKey::decode_from(&mut reader)?;
        let payload = BlobId::new(
            Digest::try_from(reader.read(32)?)
                .expect("the exact digest width was requested from the reader"),
        );
        let payload_size = reader.read_u64()?;
        let link_count = reader.read_u64()?;
        let mut links = Vec::new();
        for _ in 0..link_count {
            links.push(ObjectKey::decode_from(&mut reader)?);
        }
        reader.finish()?;
        Ok(Self::new(key, payload, payload_size, links)?)
    }
}

/// A durable opaque token identifying one logical repository state.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RepositoryRevision([u8; 32]);

/// Why a textual repository revision is invalid.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RepositoryRevisionError {
    /// The `rev-` prefix is mandatory.
    #[error("invalid revision type (expected `rev-` prefix)")]
    InvalidPrefix,
    /// The unpadded URL-safe base64 body is malformed.
    #[error("invalid revision base64: {0}")]
    InvalidBase64(String),
    /// A revision is exactly 32 bytes.
    #[error("invalid revision length: {0} (expected 32)")]
    InvalidLength(usize),
}

impl RepositoryRevision {
    /// Construct a revision token from its exact durable bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The exact durable bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for RepositoryRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rev-{}", BASE64URL_NOPAD.encode(&self.0))
    }
}

impl fmt::Debug for RepositoryRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RepositoryRevision")
            .field(&self.to_string())
            .finish()
    }
}

impl FromStr for RepositoryRevision {
    type Err = RepositoryRevisionError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let body = value
            .strip_prefix("rev-")
            .ok_or(RepositoryRevisionError::InvalidPrefix)?;
        let decoded = BASE64URL_NOPAD
            .decode(body.as_bytes())
            .map_err(|error| RepositoryRevisionError::InvalidBase64(error.to_string()))?;
        let length = decoded.len();
        let bytes = decoded
            .try_into()
            .map_err(|_| RepositoryRevisionError::InvalidLength(length))?;
        Ok(Self(bytes))
    }
}

/// The position of a logical repository state in its repository's commit
/// order.
///
/// [`RepositoryRevision`] identifies a state but is deliberately random and
/// unordered. The generation orders states: every successful commit of a
/// repository advances it, atomically with the new revision, and it never
/// decreases. Of two states of one repository, the one with the larger
/// generation includes every commit of the other, and equal generations
/// denote the same state. Generations of different repositories, or of a
/// repository recreated at the same location, are unrelated.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositoryGeneration(u64);

impl RepositoryGeneration {
    pub(crate) const fn new(generation: u64) -> Self {
        Self(generation)
    }

    /// The generation as a number.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for RepositoryGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gen-{}", self.0)
    }
}

/// Errors decoding the frozen logical key and root layouts.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum LogicalEncodingError {
    /// The input ended in the middle of a field.
    #[error("unexpected end of logical encoding")]
    UnexpectedEof,
    /// A declared byte-string length exceeds the format's fixed ceiling.
    #[error("declared byte-string length {declared} exceeds limit {limit}")]
    LengthLimit {
        /// The untrusted declared length.
        declared: u64,
        /// The applicable fixed limit.
        limit: usize,
    },
    /// A length cannot be represented on this platform.
    #[error("declared byte-string length does not fit this platform")]
    LengthOverflow,
    /// The input contained bytes after the one expected value.
    #[error("trailing bytes after logical encoding")]
    TrailingBytes,
    /// A UTF-8 field was malformed.
    #[error("root name is not valid UTF-8")]
    InvalidUtf8,
    /// The namespace is invalid.
    #[error(transparent)]
    Namespace(#[from] NamespaceIdError),
    /// The object key is invalid.
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    /// The root name is invalid.
    #[error(transparent)]
    RootName(#[from] RootNameError),
    /// The object record is structurally noncanonical.
    #[error(transparent)]
    ObjectRecord(#[from] ObjectRecordError),
}

fn encode_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) {
    encoded.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    encoded.extend_from_slice(bytes);
}

struct EncodingReader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> EncodingReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn read(&mut self, length: usize) -> Result<&'a [u8], LogicalEncodingError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(LogicalEncodingError::LengthOverflow)?;
        if end > self.bytes.len() {
            return Err(LogicalEncodingError::UnexpectedEof);
        }
        let value = &self.bytes[self.position..end];
        self.position = end;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64, LogicalEncodingError> {
        Ok(u64::from_le_bytes(
            self.read(8)?
                .try_into()
                .expect("eight bytes were requested"),
        ))
    }

    fn read_bounded_bytes(&mut self, limit: usize) -> Result<&'a [u8], LogicalEncodingError> {
        let declared = self.read_u64()?;
        if declared > limit as u64 {
            return Err(LogicalEncodingError::LengthLimit { declared, limit });
        }
        let length = usize::try_from(declared).map_err(|_| LogicalEncodingError::LengthOverflow)?;
        self.read(length)
    }

    fn finish(self) -> Result<(), LogicalEncodingError> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(LogicalEncodingError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_grammar_is_exact() {
        for valid in ["casita.blob.v1", "git-sha1.commit.v2", "a.v0"] {
            assert_eq!(NamespaceId::try_from(valid).unwrap().as_str(), valid);
        }
        for invalid in [
            "",
            "v1",
            "Casita.blob.v1",
            "casita..v1",
            "casita.blob",
            "casita.blob.v",
            "casita.blob.v1x",
            "-casita.blob.v1",
            "casita_blob.v1",
        ] {
            assert!(
                NamespaceId::try_from(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn object_key_frozen_vector() {
        let key = ObjectKey::blob(BlobId::new(Digest::from([0xa5; 32])));
        let mut expected = Vec::new();
        expected.extend_from_slice(&14u64.to_le_bytes());
        expected.extend_from_slice(b"casita.blob.v1");
        expected.extend_from_slice(&32u64.to_le_bytes());
        expected.extend_from_slice(&[0xa5; 32]);
        assert_eq!(key.encode(), expected);
        assert_eq!(ObjectKey::decode(&expected).unwrap(), key);
    }

    #[test]
    fn object_key_decoder_rejects_limits_and_trailing_bytes() {
        let mut overlong = Vec::new();
        overlong.extend_from_slice(&65u64.to_le_bytes());
        assert!(matches!(
            ObjectKey::decode(&overlong),
            Err(LogicalEncodingError::LengthLimit { .. })
        ));

        let key = ObjectKey::blob(BlobId::new(Digest::from([1; 32])));
        let mut trailing = key.encode();
        trailing.push(0);
        assert_eq!(
            ObjectKey::decode(&trailing).unwrap_err(),
            LogicalEncodingError::TrailingBytes
        );
    }

    #[test]
    fn root_names_validate_and_preserve_utf8() {
        let name = RootName::try_from("profiles/开发/hello, world!").unwrap();
        assert_eq!(name.as_str(), "profiles/开发/hello, world!");
        assert!(RootName::try_from("a//b").is_err());
        assert!(RootName::try_from("a/../b").is_err());
        assert!(RootName::try_from("a/\u{7f}").is_err());
        assert!(RootName::try_from(format!("a/{}", "x".repeat(256))).is_err());
    }

    #[test]
    fn root_record_frozen_vector() {
        let record = RootRecord::new(
            RootName::try_from("perfiles/niño!").unwrap(),
            ObjectKey::directory(DirectoryId::new(Digest::from([0x3c; 32]))),
        );
        let mut expected = Vec::new();
        expected.extend_from_slice(&15u64.to_le_bytes());
        expected.extend_from_slice("perfiles/niño!".as_bytes());
        expected.extend_from_slice(&19u64.to_le_bytes());
        expected.extend_from_slice(b"casita.directory.v1");
        expected.extend_from_slice(&32u64.to_le_bytes());
        expected.extend_from_slice(&[0x3c; 32]);
        assert_eq!(record.encode(), expected);
        assert_eq!(RootRecord::decode(&expected).unwrap(), record);
    }

    #[test]
    fn records_require_canonical_links() {
        let first = ObjectKey::blob(BlobId::new(Digest::from([1; 32])));
        let second = ObjectKey::blob(BlobId::new(Digest::from([2; 32])));
        let key = ObjectKey::directory(DirectoryId::new(Digest::from([3; 32])));
        let payload = BlobId::new(Digest::from([3; 32]));
        assert!(
            ObjectRecord::new(key.clone(), payload, 0, vec![first.clone(), second.clone()]).is_ok()
        );
        assert_eq!(
            ObjectRecord::new(key, payload, 0, vec![second, first]).unwrap_err(),
            ObjectRecordError::NonCanonicalLinks
        );
    }

    #[test]
    fn cloned_records_share_immutable_links() {
        let link = ObjectKey::blob(BlobId::new(Digest::from([1; 32])));
        let record = ObjectRecord::new(
            ObjectKey::directory(DirectoryId::new(Digest::from([2; 32]))),
            BlobId::new(Digest::from([3; 32])),
            0,
            vec![link],
        )
        .unwrap();
        let cloned = record.clone();

        assert!(Arc::ptr_eq(&record.links, &cloned.links));
        assert_eq!(record.encode(), cloned.encode());
    }

    #[test]
    fn revision_text_roundtrip() {
        let revision = RepositoryRevision::from_bytes([0x5a; 32]);
        assert_eq!(
            revision.to_string().parse::<RepositoryRevision>().unwrap(),
            revision
        );
        assert!(!revision.to_string().contains('='));
    }
}
