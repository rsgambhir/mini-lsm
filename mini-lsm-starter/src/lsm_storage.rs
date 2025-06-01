#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    // Making this an Arc<LsmStorageState> protected by a Rwlock avoids cloning the LsmStorageState in the read paths(scan, get, put).
    // The writers(compaction, freezing, etc.) will clone the LsmStorageState and update the Arc, which is protected by RwLock.
    // Arc<LsmStorageState> serves as a snapshot of the lsm-state and writers update the snapshot as a whole.
    // todo(ramneek): can state be Atomic_Arc?
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    // To update state: hold state_lock lock -> read, state, clone, update and update the arc
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        todo!()
        // self.flush_thread.
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let state = { self.state.read().clone() };

        match std::iter::once(&state.memtable)
            .chain(state.imm_memtables.iter())
            .find_map(|t| t.get(_key))
        {
            Some(v) if !v.is_empty() => return Ok(Some(v)),
            Some(v) if v.is_empty() => return Ok(None),
            _ => {}
        }

        let key_fp = farmhash::fingerprint32(_key);

        for sst in state
            .l0_sstables
            .iter()
            .map(|id| state.sstables.get(id).unwrap().clone())
            .filter(|sst| sst.first_key().raw_ref() <= _key && _key <= sst.last_key().raw_ref())
            .filter(|sst| sst.bloom.as_ref().is_none_or(|b| b.may_contain(key_fp)))
        {
            let itr: SsTableIterator =
                SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(_key))?;
            if itr.is_valid() && itr.key().raw_ref() == _key {
                let val = itr.value();
                return if val.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(Bytes::copy_from_slice(val)))
                };
            }
        }

        for (_, level_ssts) in &state.levels {
            // todo(ramneek): use binary search to find the sstable in the level...
            if let Some(sst) = level_ssts
                .iter()
                .map(|id| state.sstables.get(id).unwrap().clone())
                .find(|sst| sst.first_key().raw_ref() <= _key && _key <= sst.last_key().raw_ref())
                .filter(|sst| sst.bloom.as_ref().is_none_or(|b| b.may_contain(key_fp)))
            {
                let itr = SsTableIterator::<false>::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(_key),
                )?;
                if itr.is_valid() && itr.key().raw_ref() == _key {
                    let val = itr.value();
                    if !val.is_empty() {
                        return Ok(Some(Bytes::copy_from_slice(val)));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    fn try_freeze(&self, size_hint: usize) -> Result<()> {
        if size_hint > self.options.target_sst_size {
            let guard = self.state_lock.lock();
            let state = self.state.read();
            if state.memtable.approximate_size() > self.options.target_sst_size {
                drop(state);
                return self.force_freeze_memtable(&guard);
            }
        }
        Ok(())
    }

    fn _put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let state = self.state.read();
        let res = state.memtable.put(_key, _value);
        let sz = state.memtable.approximate_size();
        drop(state);
        self.try_freeze(sz).expect("failed to freeze");
        res
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        self._put(_key, _value)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self._put(_key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let next = Arc::new(MemTable::create(self.next_sst_id()));
        // hold write lock to prevent new puts, todo(ramneek): reduce scope of write lock
        let mut sp = self.state.write();
        let mut new_state = sp.as_ref().clone();
        new_state.imm_memtables.insert(0, sp.memtable.clone());
        new_state.memtable = next;
        *sp = Arc::new(new_state);
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let last_memtable = { self.state.read().imm_memtables.last().unwrap().clone() };

        let mut builder = SsTableBuilder::new(self.options.block_size);
        last_memtable.flush(&mut builder)?;
        let sst = builder.build(
            last_memtable.id(),
            Some(self.block_cache.clone()),
            self.path_of_sst(last_memtable.id()),
        )?;

        let mut new_state = { self.state.read().as_ref().clone() };
        new_state.imm_memtables.pop();
        new_state.l0_sstables.insert(0, sst.sst_id());
        new_state.sstables.insert(sst.sst_id(), Arc::new(sst));

        *self.state.write() = Arc::new(new_state);
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = { self.state.read().clone() };

        let mem_tables_itr = MergeIterator::create(
            std::iter::once(&state.memtable)
                .chain(state.imm_memtables.iter())
                .map(|mt: &Arc<MemTable>| Box::new(mt.clone().scan(lower, upper)))
                .collect(),
        );

        let l0_itr = {
            let mut l0_itrs = Vec::with_capacity(state.l0_sstables.len());
            for sst in state
                .l0_sstables
                .iter()
                .map(|id| state.sstables.get(id).unwrap().clone())
                .filter(|sst| sst_overlaps_range(sst, lower, upper))
            {
                let itr = sst_seek_to_start(sst.clone(), lower)?;
                l0_itrs.push(Box::new(itr));
            }
            MergeIterator::create(l0_itrs)
        };

        let levels_itr = {
            let mut level_itrs = Vec::with_capacity(state.levels.len());
            for (_, ssts) in &state.levels {
                // todo(ramneek): use binary search
                let ssts: Vec<Arc<SsTable>> = ssts
                    .iter()
                    .map(|id| state.sstables.get(id).unwrap().clone())
                    .filter(|sst| sst_overlaps_range(sst, lower, upper))
                    .collect();
                if ssts.is_empty() {
                    continue;
                }
                level_itrs.push(Box::new(match lower {
                    Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts)?,
                    Bound::Included(key) => {
                        SstConcatIterator::create_and_seek_to_key(ssts, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut itr = SstConcatIterator::create_and_seek_to_key(
                            ssts,
                            KeySlice::from_slice(key),
                        )?;
                        if itr.is_valid() && itr.key().raw_ref() == key {
                            itr.next()?;
                        }
                        itr
                    }
                }))
            }
            MergeIterator::create(level_itrs)
        };

        let itr = TwoMergeIterator::create(mem_tables_itr, l0_itr)?;
        let itr = TwoMergeIterator::create(itr, levels_itr)?;
        LsmIterator::new(itr, upper).map(FusedIterator::new)
    }
}

enum SeekPos<'a> {
    Start,
    Key(KeySlice<'a>, bool),
    None,
}
fn sst_overlaps_range(sst: &Arc<SsTable>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    // intersection with Lower Bound -> inf
    match lower {
        Bound::Included(skey) if skey > sst.last_key().raw_ref() => return false,
        Bound::Excluded(skey) if skey >= sst.last_key().raw_ref() => return false,
        _ => {}
    }

    // intersection with -inf -> upper bound
    match upper {
        Bound::Included(ekey) if ekey < sst.first_key().raw_ref() => return false,
        Bound::Excluded(ekey) if ekey <= sst.first_key().raw_ref() => return false,
        _ => {}
    }

    true
}

fn sst_seek_to_start(sst: Arc<SsTable>, start: Bound<&[u8]>) -> Result<SsTableIterator> {
    match start {
        Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst),
        Bound::Included(skey) if skey == sst.first_key().raw_ref() => {
            SsTableIterator::create_and_seek_to_first(sst)
        }
        Bound::Included(skey) => {
            SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(skey))
        }
        Bound::Excluded(skey) => {
            let mut itr = SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(skey))?;
            itr.next()?;
            Ok(itr)
        }
    }
}
