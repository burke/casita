use std::{
    ptr::{NonNull, null_mut},
    sync::Mutex,
    time::Instant,
};

use libc::{EINVAL, EIO, ENOENT, EROFS};
use objc2::{AnyThread, DefinedClass, define_class, msg_send, rc::Retained};
use objc2_foundation::{
    NSArray, NSData, NSError, NSInteger, NSObject, NSObjectProtocol, NSString, NSUUID,
};
use objc2_fs_kit::{
    FSDeactivateOptions, FSDirectoryCookie, FSDirectoryEntryPacker, FSDirectoryVerifier,
    FSFileName, FSItem, FSItemAttributes, FSItemGetAttributesRequest, FSItemID,
    FSItemSetAttributesRequest, FSItemType, FSMutableFileDataBuffer, FSSetXattrPolicy,
    FSStatFSResult, FSSyncFlags, FSTaskOptions, FSVolume, FSVolumeIdentifier, FSVolumeOperations,
    FSVolumePathConfOperations, FSVolumeReadWriteOperations, FSVolumeSupportedCapabilities,
    FSVolumeXattrOperations,
};
use tracing::trace;

use super::{
    item::Item,
    util::{io_err, posix_err},
};
use crate::{
    BackendSession, DirectoryEntry, DirectorySnapshot, FileKind, Filesystem, Observation,
    VolumeOptions,
};

macro_rules! backend {
    ($this:expr, $reply:expr, $failure:expr) => {
        match $this.ivars().backend.acquire() {
            Ok(backend) => backend,
            Err(error) => {
                $reply.call(($failure)(io_err(&error)));
                return;
            }
        }
    };
}
pub struct VolumeState {
    backend: BackendSession<Box<dyn Filesystem>>,
    options: VolumeOptions,
    // Retain a bounded number of pagination snapshots. Evicted verifiers return
    // ESTALE so the caller restarts, rather than silently reusing old cookies.
    enumerations: Mutex<crate::enumeration::Enumerations>,
}
fn filename(bytes: &[u8]) -> Retained<FSFileName> {
    unsafe { FSFileName::nameWithData(&NSData::with_bytes(bytes)) }
}
fn filename_from_bytes(bytes: &[u8]) -> Retained<FSFileName> {
    // FSKit copies exactly this bounded byte sequence (up to any NUL).
    // A slice pointer is non-null, including for an empty slice.
    unsafe {
        FSFileName::initWithBytes_length(
            FSFileName::alloc(),
            NonNull::new(bytes.as_ptr().cast_mut().cast()).unwrap(),
            bytes.len(),
        )
    }
}
fn validate_filename_constructors() {
    // This gate runs before a volume is exposed, outside timed enumeration.
    // Check raw bytes and ownership after the original buffer is overwritten.
    for expected in [
        vec![],
        b"plain".to_vec(),
        b"._sidecar".to_vec(),
        b"raw-\xff\xfe".to_vec(),
        "caf\u{e9}".as_bytes().to_vec(),
        vec![b'x'; 255],
    ] {
        for constructor in [filename, filename_from_bytes] {
            let mut source = expected.clone();
            let name = constructor(&source);
            source.fill(b'!');
            drop(source);
            let data = unsafe { name.data() };
            assert_eq!(unsafe { data.as_bytes_unchecked() }, expected.as_slice());
        }
    }
}

define_class!(
    #[unsafe(super(FSVolume, NSObject))]
    #[ivars = VolumeState]
    pub(crate) struct Volume;

    unsafe impl NSObjectProtocol for Volume {}

    #[allow(non_snake_case)]
    unsafe impl FSVolumePathConfOperations for Volume {
        // _PC_LINK_MAX
        #[unsafe(method(maximumLinkCount))]
        fn maximumLinkCount(&self) -> NSInteger {
            trace!("maximumLinkCount");
            1
        }

        // _PC_NAME_MAX
        #[unsafe(method(maximumNameLength))]
        fn maximumNameLength(&self) -> NSInteger {
            trace!("maximumNameLength");
            255
        }

        // _PC_CHOWN_RESTRICTED
        #[unsafe(method(restrictsOwnershipChanges))]
        fn restrictsOwnershipChanges(&self) -> bool {
            trace!("restrictsOwnershipChanges");
            true
        }

        // _PC_NO_TRUNC
        #[unsafe(method(truncatesLongNames))]
        fn truncatesLongNames(&self) -> bool {
            trace!("truncatesLongNames");
            false
        }

        // _PC_FILESIZEBITS
        #[unsafe(method(maximumXattrSizeInBits))]
        fn maximumXattrSizeInBits(&self) -> NSInteger {
            trace!("maximumXattrSizeInBits");
            0
        }

        // _PC_XATTR_SIZE_BITS
        #[unsafe(method(maximumFileSizeInBits))]
        fn maximumFileSizeInBits(&self) -> u64 {
            trace!("maximumFileSizeInBits");
            64
        }
    }

    #[allow(non_snake_case)]
    unsafe impl FSVolumeOperations for Volume {
        #[unsafe(method_id(supportedVolumeCapabilities))]
        fn supportedVolumeCapabilities(&self) -> Retained<FSVolumeSupportedCapabilities> {
            trace!("supportedVolumeCapabilities");
            let capabilities = unsafe { FSVolumeSupportedCapabilities::new() };
            unsafe { capabilities.setSupportsSymbolicLinks(true) };
            if self.ivars().options.explicit_capabilities {
                // Declare only behavior this prototype implements. IDs are
                // 64-bit but mount-local, not persistent across remounts.
                unsafe {
                    capabilities.setCaseFormat(objc2_fs_kit::FSVolumeCaseFormat::Sensitive);
                    capabilities.setSupports64BitObjectIDs(true);
                    capabilities.setSupportsFastStatFS(true);
                    capabilities.setDoesNotSupportRootTimes(true);
                }
            }
            capabilities
        }

        #[unsafe(method_id(volumeStatistics))]
        fn volumeStatistics(&self) -> Retained<FSStatFSResult> {
            trace!("volumeStatistics");
            let stats = unsafe {
                FSStatFSResult::initWithFileSystemTypeName(
                    FSStatFSResult::alloc(),
                    &NSString::from_str(super::configuration().filesystem_type), // FSShortName
                )
            };

            unsafe {
                stats.setBlockSize(4096);
                stats.setIoSize(65536);
                stats.setTotalBlocks(1 << 20);
                stats.setAvailableBlocks(0);
                stats.setFreeBlocks(0);
                stats.setTotalFiles(1 << 20);
                stats.setFreeFiles(0);
            }

            stats
        }

        #[unsafe(method(activateWithOptions:replyHandler:))]
        fn activateWithOptions_replyHandler(
            &self,
            options: &FSTaskOptions,
            reply: &block2::DynBlock<dyn Fn(*mut FSItem, *mut NSError)>,
        ) {
            trace!(taskOptions = ?options, "activate");
            let backend = backend!(self, reply, |error| (null_mut(), error));
            match backend.metadata(backend.root()) {
                Ok(entry) => {
                    let item =
                        Item::new(&entry, self.ivars().options.store_timestamps).into_super();
                    reply.call((Retained::as_ptr(&item).cast_mut(), null_mut()));
                }
                Err(error) => reply.call((null_mut(), io_err(&error))),
            }
        }

        #[unsafe(method(deactivateWithOptions:replyHandler:))]
        fn deactivateWithOptions_replyHandler(
            &self,
            options: FSDeactivateOptions,
            reply: &block2::DynBlock<dyn Fn(*mut NSError)>,
        ) {
            trace!(?options, "deactivate");
            match self.close() {
                Ok(()) => reply.call((null_mut(),)),
                Err(error) => reply.call((io_err(&error),)),
            }
        }

        #[unsafe(method(mountWithOptions:replyHandler:))]
        fn mountWithOptions_replyHandler(
            &self,
            options: &FSTaskOptions,
            reply: &block2::DynBlock<dyn Fn(*mut NSError)>,
        ) {
            trace!(taskOptions = ?options, "mount");
            reply.call((null_mut(),));
        }

        #[unsafe(method(unmountWithReplyHandler:))]
        fn unmountWithReplyHandler(&self, reply: &block2::DynBlock<dyn Fn()>) {
            trace!("unmount");
            reply.call(());
        }

        #[unsafe(method(synchronizeWithFlags:replyHandler:))]
        fn synchronizeWithFlags_replyHandler(
            &self,
            flags: FSSyncFlags,
            reply: &block2::DynBlock<dyn Fn(*mut NSError)>,
        ) {
            trace!(?flags, "synchronize");
            reply.call((null_mut(),));
        }

        #[unsafe(method(getAttributes:ofItem:replyHandler:))]
        fn getAttributes_ofItem_replyHandler(
            &self,
            desired_attributes: &FSItemGetAttributesRequest,
            item: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut FSItemAttributes, *mut NSError)>,
        ) {
            let item = item.downcast_ref::<Item>().unwrap();
            trace!(wantedAttributes = ?unsafe { desired_attributes.wantedAttributes() }, ?item, "attributes");
            let attributes: *const FSItemAttributes = &**item.attributes();
            reply.call((attributes.cast_mut(), null_mut()));
        }

        #[unsafe(method(setAttributes:onItem:replyHandler:))]
        fn setAttributes_onItem_replyHandler(
            &self,
            new_attributes: &FSItemSetAttributesRequest,
            item: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut FSItemAttributes, *mut NSError)>,
        ) {
            let item = item.downcast_ref::<Item>().unwrap();
            trace!(?new_attributes, ?item, "setAttributes");
            reply.call((null_mut(), posix_err(EROFS)));
        }

        #[unsafe(method(lookupItemNamed:inDirectory:replyHandler:))]
        fn lookupItemNamed_inDirectory_replyHandler(
            &self,
            name: &FSFileName,
            directory: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut FSItem, *mut FSFileName, *mut NSError)>,
        ) {
            let directory = directory.downcast_ref::<Item>().unwrap();
            trace!(?name, ?directory, "lookupItem");
            let data = unsafe { name.data() };
            let bytes = unsafe { data.as_bytes_unchecked() };
            let backend = backend!(self, reply, |error| (null_mut(), null_mut(), error));
            let parent = unsafe { directory.attributes().fileID() }.0;
            let result = backend.lookup(parent, bytes);
            match result {
                Ok(Some(entry)) => {
                    let item =
                        Item::new(&entry, self.ivars().options.store_timestamps).into_super();
                    reply.call((
                        Retained::as_ptr(&item).cast_mut(),
                        name as *const _ as *mut _,
                        null_mut(),
                    ));
                }
                Ok(None) => reply.call((null_mut(), null_mut(), posix_err(ENOENT))),
                Err(error) => {
                    eprintln!("lookup: {error:#}");
                    reply.call((null_mut(), null_mut(), io_err(&error)));
                }
            }
        }

        #[unsafe(method(reclaimItem:replyHandler:))]
        fn reclaimItem_replyHandler(
            &self,
            item: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut NSError)>,
        ) {
            let item = item.downcast_ref::<Item>().unwrap();
            trace!(?item, "reclaimItem");
            reply.call((null_mut(),));
        }

        #[unsafe(method(readSymbolicLink:replyHandler:))]
        fn readSymbolicLink_replyHandler(
            &self,
            item: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut FSFileName, *mut NSError)>,
        ) {
            let item = item.downcast_ref::<Item>().unwrap();
            trace!(?item, "readSymbolicLink");
            let id = unsafe { item.attributes().fileID() }.0;
            let backend = backend!(self, reply, |error| (null_mut(), error));
            match backend.read_link(id) {
                Ok(bytes) => {
                    let target = filename(&bytes);
                    reply.call((Retained::as_ptr(&target).cast_mut(), null_mut()));
                }
                Err(error) => reply.call((null_mut(), io_err(&error))),
            }
        }

        #[unsafe(method(createItemNamed:type:inDirectory:attributes:replyHandler:))]
        fn createItemNamed_type_inDirectory_attributes_replyHandler(
            &self,
            name: &FSFileName,
            r#type: FSItemType,
            directory: &FSItem,
            new_attributes: &FSItemSetAttributesRequest,
            reply: &block2::DynBlock<dyn Fn(*mut FSItem, *mut FSFileName, *mut NSError)>,
        ) {
            let directory = directory.downcast_ref::<Item>().unwrap();
            let kind = match r#type {
                FSItemType::Directory => FileKind::Directory,
                FSItemType::File => FileKind::File,
                _ => {
                    reply.call((null_mut(), null_mut(), posix_err(EROFS)));
                    return;
                }
            };
            let data = unsafe { name.data() };
            let backend = backend!(self, reply, |error| (null_mut(), null_mut(), error));
            match backend.create(
                unsafe { directory.attributes().fileID() }.0,
                unsafe { data.as_bytes_unchecked() },
                kind,
                None,
            ) {
                Ok(entry) => {
                    let item =
                        Item::new(&entry, self.ivars().options.store_timestamps).into_super();
                    reply.call((
                        Retained::as_ptr(&item).cast_mut(),
                        name as *const _ as *mut _,
                        null_mut(),
                    ));
                }
                Err(error) => reply.call((null_mut(), null_mut(), io_err(&error))),
            }
        }

        #[unsafe(method(createSymbolicLinkNamed:inDirectory:attributes:linkContents:replyHandler:))]
        fn createSymbolicLinkNamed_inDirectory_attributes_linkContents_replyHandler(
            &self,
            name: &FSFileName,
            directory: &FSItem,
            new_attributes: &FSItemSetAttributesRequest,
            contents: &FSFileName,
            reply: &block2::DynBlock<dyn Fn(*mut FSItem, *mut FSFileName, *mut NSError)>,
        ) {
            let directory = directory.downcast_ref::<Item>().unwrap();
            trace!(
                ?name,
                ?directory,
                ?new_attributes,
                ?contents,
                "createSymbolicLink"
            );
            let data = unsafe { name.data() };
            let target = unsafe { contents.data() };
            let backend = backend!(self, reply, |error| (null_mut(), null_mut(), error));
            match backend.create(
                unsafe { directory.attributes().fileID() }.0,
                unsafe { data.as_bytes_unchecked() },
                FileKind::Symlink,
                Some(unsafe { target.as_bytes_unchecked() }),
            ) {
                Ok(entry) => {
                    let item =
                        Item::new(&entry, self.ivars().options.store_timestamps).into_super();
                    reply.call((
                        Retained::as_ptr(&item).cast_mut(),
                        name as *const _ as *mut _,
                        null_mut(),
                    ));
                }
                Err(error) => reply.call((null_mut(), null_mut(), io_err(&error))),
            }
        }

        #[unsafe(method(createLinkToItem:named:inDirectory:replyHandler:))]
        fn createLinkToItem_named_inDirectory_replyHandler(
            &self,
            item: &FSItem,
            name: &FSFileName,
            directory: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut FSFileName, *mut NSError)>,
        ) {
            let item = item.downcast_ref::<Item>().unwrap();
            let directory = directory.downcast_ref::<Item>().unwrap();
            trace!(?item, ?name, ?directory, "createLink");
            reply.call((null_mut(), posix_err(EROFS)));
        }

        #[unsafe(method(removeItem:named:fromDirectory:replyHandler:))]
        fn removeItem_named_fromDirectory_replyHandler(
            &self,
            item: &FSItem,
            name: &FSFileName,
            directory: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut NSError)>,
        ) {
            let item = item.downcast_ref::<Item>().unwrap();
            let directory = directory.downcast_ref::<Item>().unwrap();
            trace!(?item, ?name, ?directory, "removeItem");
            reply.call((posix_err(EROFS),));
        }

        #[allow(clippy::too_many_arguments)]
        #[unsafe(method(renameItem:inDirectory:named:toNewName:inDirectory:overItem:replyHandler:))]
        fn renameItem_inDirectory_named_toNewName_inDirectory_overItem_replyHandler(
            &self,
            item: &FSItem,
            source_directory: &FSItem,
            source_name: &FSFileName,
            destination_name: &FSFileName,
            destination_directory: &FSItem,
            over_item: Option<&FSItem>,
            reply: &block2::DynBlock<dyn Fn(*mut FSFileName, *mut NSError)>,
        ) {
            let item = item.downcast_ref::<Item>().unwrap();
            let source_directory = source_directory.downcast_ref::<Item>().unwrap();
            let destination_directory = destination_directory.downcast_ref::<Item>().unwrap();
            trace!(
                ?item,
                ?source_directory,
                ?source_name,
                ?destination_name,
                ?destination_directory,
                ?over_item,
                "renameItem",
            );
            reply.call((null_mut(), posix_err(EROFS)));
        }

        #[unsafe(method(enumerateDirectory:startingAtCookie:verifier:providingAttributes:usingPacker:replyHandler:))]
        fn enumerateDirectory_startingAtCookie_verifier_providingAttributes_usingPacker_replyHandler(
            &self,
            directory: &FSItem,
            cookie: FSDirectoryCookie,
            verifier: FSDirectoryVerifier,
            attributes: Option<&FSItemGetAttributesRequest>,
            packer: &FSDirectoryEntryPacker,
            reply: &block2::DynBlock<dyn Fn(FSDirectoryVerifier, *mut NSError)>,
        ) {
            let directory = directory.downcast_ref::<Item>().unwrap();
            trace!(
                ?directory,
                ?cookie,
                ?verifier,
                ?attributes,
                ?packer,
                "enumerateDirectory",
            );
            let backend = backend!(self, reply, |error| (verifier, error));
            let parent = unsafe { directory.attributes().fileID() }.0;
            let started = Instant::now();
            let with_attributes = attributes.is_some();
            let snapshot = self.enumeration(&**backend, parent, cookie, verifier, with_attributes);
            let (verifier, entries) = match snapshot {
                Ok(value) => value,
                Err(error) => {
                    reply.call((verifier, io_err(&error)));
                    return;
                }
            };
            let mut packed = 0;
            let mut allocations = 0;
            let detailed = self.ivars().options.time_enumeration;
            let mut phases = [0; 5];
            if detailed {
                phases[0] = started.elapsed().as_nanos() as u64;
            }
            let mut failure = None;
            let result = entries.enumerate(cookie, |entry, next_cookie| {
                let item = if with_attributes || self.ivars().options.eager_attributes {
                    let metadata = match entry
                        .metadata
                        .map(Ok)
                        .unwrap_or_else(|| backend.metadata(entry.id))
                    {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            failure = Some(error);
                            return false;
                        }
                    };
                    allocations += 1;
                    Some(Item::new(&metadata, self.ivars().options.store_timestamps))
                } else {
                    None
                };
                let kind = match entry.kind {
                    FileKind::Directory => FSItemType::Directory,
                    FileKind::File => FSItemType::File,
                    FileKind::Symlink => FSItemType::Symlink,
                };
                let name_started = detailed.then(Instant::now);
                let name = if self.ivars().options.filename_from_bytes {
                    filename_from_bytes(&entry.name)
                } else {
                    filename(&entry.name)
                };
                if let Some(start) = name_started {
                    phases[1] += start.elapsed().as_nanos() as u64;
                }
                let pack_started = detailed.then(Instant::now);
                let accepted = unsafe {
                    packer.packEntryWithName_itemType_itemID_nextCookie_attributes(
                        &name,
                        kind,
                        FSItemID(entry.id),
                        next_cookie,
                        attributes.map(|_| &**item.as_ref().unwrap().attributes()),
                    )
                };
                if let Some(start) = pack_started {
                    phases[2] += start.elapsed().as_nanos() as u64;
                }
                let drop_started = detailed.then(Instant::now);
                drop(name);
                if let Some(start) = drop_started {
                    phases[3] += start.elapsed().as_nanos() as u64;
                    phases[4] += 1;
                }
                if accepted {
                    packed += 1;
                }
                accepted
            });
            backend.observe(Observation::Enumeration {
                parent,
                attributes: with_attributes,
                initial: cookie == 0,
                entries: packed,
                nanos: started.elapsed().as_nanos() as u64,
                allocations,
                phases: detailed.then_some(phases),
            });
            if let Some(error) = failure.or_else(|| result.err()) {
                reply.call((verifier, io_err(&error)));
            } else {
                reply.call((verifier, null_mut()));
            }
        }
    }
    // Explicitly answer an empty xattr namespace unless emulation is requested.
    unsafe impl FSVolumeXattrOperations for Volume {
        #[unsafe(method(xattrOperationsInhibited))]
        fn xattr_inhibited(&self) -> bool {
            self.ivars().options.emulate_xattrs
        }

        #[unsafe(method(getXattrNamed:ofItem:replyHandler:))]
        fn get_xattr(
            &self,
            name: &FSFileName,
            _item: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut NSData, *mut NSError)>,
        ) {
            let data = unsafe { name.data() };
            let backend = backend!(self, reply, |error| (null_mut(), error));
            backend.observe(Observation::Xattr {
                operation: "get",
                name: unsafe { data.as_bytes_unchecked() },
            });
            reply.call((null_mut(), posix_err(libc::ENOATTR)));
        }

        #[unsafe(method(listXattrsOfItem:replyHandler:))]
        fn list_xattrs(
            &self,
            _item: &FSItem,
            reply: &block2::DynBlock<dyn Fn(*mut NSArray<FSFileName>, *mut NSError)>,
        ) {
            let backend = backend!(self, reply, |error| (null_mut(), error));
            backend.observe(Observation::Xattr {
                operation: "list",
                name: b"",
            });
            let names = NSArray::<FSFileName>::new();
            reply.call((Retained::as_ptr(&names).cast_mut(), null_mut()));
        }

        #[unsafe(method(setXattrNamed:toData:onItem:policy:replyHandler:))]
        fn set_xattr(
            &self,
            name: &FSFileName,
            _value: Option<&NSData>,
            _item: &FSItem,
            _policy: FSSetXattrPolicy,
            reply: &block2::DynBlock<dyn Fn(*mut NSError)>,
        ) {
            let data = unsafe { name.data() };
            let backend = backend!(self, reply, |error| (error,));
            backend.observe(Observation::Xattr {
                operation: "set",
                name: unsafe { data.as_bytes_unchecked() },
            });
            reply.call((posix_err(EROFS),));
        }
    }
    unsafe impl FSVolumeReadWriteOperations for Volume {
        #[unsafe(method(readFromFile:offset:length:intoBuffer:replyHandler:))]
        fn read_file(
            &self,
            item: &FSItem,
            offset: libc::off_t,
            length: usize,
            buffer: &FSMutableFileDataBuffer,
            reply: &block2::DynBlock<dyn Fn(usize, *mut NSError)>,
        ) {
            let Some(item) = item.downcast_ref::<Item>() else {
                reply.call((0, posix_err(EIO)));
                return;
            };
            let id = unsafe { item.attributes().fileID() }.0;
            if offset < 0 || length > unsafe { buffer.length() } || length > u32::MAX as usize {
                reply.call((0, posix_err(EINVAL)));
                return;
            }
            let backend = backend!(self, reply, |error| (0, error));
            let callback_started = self.ivars().options.trace_reads.then(Instant::now);
            let result = backend.read_data(id, offset as u64, length);
            let backend_ns = callback_started.map(|start| start.elapsed().as_nanos() as u64);
            let copy_started = callback_started.map(|_| Instant::now());
            let response = match result {
                Ok(bytes) if bytes.len() <= length => {
                    if !bytes.is_empty() {
                        // SAFETY: FSKit supplies a writable buffer of at least
                        // `length` bytes, checked above. Source bytes are valid
                        // for the admitted backend call and do not alias it.
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                bytes.as_ptr(),
                                buffer.mutableBytes().as_ptr().cast::<u8>(),
                                bytes.len(),
                            );
                        }
                    }
                    (bytes.len(), null_mut())
                }
                Ok(_) => (0, posix_err(EIO)),
                Err(error) => (0, io_err(&error)),
            };
            let failed = !response.1.is_null();
            let copy_ns = copy_started.map(|start| start.elapsed().as_nanos() as u64);
            let reply_started = callback_started.map(|_| Instant::now());
            reply.call(response);
            if let Some(start) = reply_started {
                backend.observe(Observation::Read {
                    id,
                    backend_ns: backend_ns.unwrap(),
                    copy_ns: copy_ns.unwrap(),
                    reply_ns: start.elapsed().as_nanos() as u64,
                    error: failed,
                });
            }
        }

        #[unsafe(method(writeContents:toFile:atOffset:replyHandler:))]
        fn write_file(
            &self,
            _contents: &NSData,
            _item: &FSItem,
            _offset: libc::off_t,
            reply: &block2::DynBlock<dyn Fn(usize, *mut NSError)>,
        ) {
            reply.call((0, posix_err(EROFS)));
        }
    }
);

impl Volume {
    pub(crate) fn close(&self) -> std::io::Result<()> {
        self.ivars().backend.close()?;
        self.ivars().enumerations.lock().unwrap().clear();
        Ok(())
    }

    fn enumeration(
        &self,
        backend: &dyn Filesystem,
        parent: u64,
        cookie: u64,
        verifier: u64,
        attributes: bool,
    ) -> std::io::Result<(u64, DirectorySnapshot)> {
        let mut snapshots = self.ivars().enumerations.lock().unwrap();
        if cookie != 0 {
            return snapshots
                .get(verifier, parent, attributes)
                .map(|entries| (verifier, entries))
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::ESTALE));
        }
        // Do backend work without serializing enumeration of unrelated directories.
        drop(snapshots);
        let mut entries = backend.directory(parent)?;
        if !attributes {
            let dot = backend.metadata(parent)?;
            let dotdot = backend.metadata(if parent == backend.root() {
                parent
            } else {
                dot.parent
            })?;
            let mut all = Vec::with_capacity(entries.0.len() + 2);
            for (name, metadata) in [(b".".to_vec(), dot), (b"..".to_vec(), dotdot)] {
                all.push(DirectoryEntry {
                    name,
                    id: metadata.id,
                    kind: metadata.kind,
                    metadata: Some(metadata),
                });
            }
            all.extend(entries.0.iter().cloned());
            entries = DirectorySnapshot(all.into());
        }
        snapshots = self.ivars().enumerations.lock().unwrap();
        let verifier = snapshots.insert(parent, attributes, entries.clone())?;
        Ok((verifier, entries))
    }

    pub(crate) fn new(backend: Box<dyn Filesystem>) -> Retained<Self> {
        validate_filename_constructors();
        let options = backend.options();
        let this = Self::alloc().set_ivars(VolumeState {
            backend: BackendSession::new(backend),
            options,
            enumerations: Mutex::new(crate::enumeration::Enumerations::default()),
        });
        let volume_id = unsafe {
            FSVolumeIdentifier::initWithUUID(FSVolumeIdentifier::alloc(), &NSUUID::UUID())
        };
        let name = filename(super::configuration().name.as_bytes());
        unsafe { msg_send![super(this), initWithVolumeID: &*volume_id, volumeName: &*name] }
    }
}
