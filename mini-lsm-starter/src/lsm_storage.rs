use std::collections::{BTreeSet, HashMap};
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
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_key_ref_ts_bound, MemTable};
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
use bytes::Bytes;
use itertools::Itertools;
use parking_lot::{Mutex, MutexGuard, RwLock};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

const MANIFEST_FILE_NAME: &str = "MANIFEST";

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

    fn get_all_sst_ids(&self) -> Vec<usize> {
        self.l0_sstables
            .iter()
            .chain(
                self.levels
                    .iter()
                    .flat_map(|(_, level_ssts)| level_ssts.iter()),
            )
            .copied()
            .collect()
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
        self.flush_notifier.send(()).ok();
        if let Some(flush_thread) = self.flush_thread.lock().take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        self.compaction_notifier.send(()).ok();
        if let Some(compaction_thread) = self.compaction_thread.lock().take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            self.inner.sync()?
        } else {
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?
            }

            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }
        Ok(())
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

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
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

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
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

    pub(crate) fn mvcc(self: &Arc<Self>) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    fn clean_up_unreferenced_ssts(&self) -> Result<()> {
        // todo
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let mut state = LsmStorageState::create(&options);

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

        let block_cache = Arc::new(BlockCache::new(1024));

        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let manifest;
        let mut ssts_to_del: Vec<usize> = Vec::new();
        let mut memtables = BTreeSet::new();

        let manifest_path = path.join(MANIFEST_FILE_NAME);
        if !manifest_path.exists() {
            manifest = Manifest::create(manifest_path)?;
        } else {
            let records;
            (manifest, records) = Manifest::recover(manifest_path)?;

            for record in records {
                match record {
                    ManifestRecord::NewMemtable(mem_table_id) => {
                        assert!(options.enable_wal);
                        memtables.insert(mem_table_id);
                    }
                    ManifestRecord::Flush(sst_id) => {
                        memtables.remove(&sst_id);
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id)
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]))
                        }
                    }
                    ManifestRecord::Compaction(task, compaction_result) => {
                        let (new_state, mut del) = compaction_controller.apply_compaction_result(
                            &state,
                            &task,
                            &compaction_result,
                            true,
                        );
                        state = new_state;
                        ssts_to_del.append(&mut del);
                    }
                }
            }
        }

        // recover ssts
        let sst_ids = state.get_all_sst_ids();
        for sst_id in &sst_ids {
            let sst_file = FileObject::open(&Self::path_of_sst_static(path, *sst_id))?;
            let sst = SsTable::open(*sst_id, Some(block_cache.clone()), sst_file)?;
            state.sstables.insert(*sst_id, Arc::new(sst));
        }
        assert!(state.l0_sstables.iter().rev().is_sorted());
        state.levels.iter_mut().for_each(|(_, level_ssts)| {
            *level_ssts = level_ssts
                .iter()
                .sorted_by_key(|id| state.sstables.get(id).unwrap().first_key())
                .copied()
                .collect()
        });

        // recover memtables
        for mt_id in memtables.iter() {
            assert!(options.enable_wal);
            let memtable =
                MemTable::recover_from_wal(*mt_id, Self::path_of_wal_static(path, *mt_id))?;
            if !memtable.is_empty() {
                state.imm_memtables.insert(0, Arc::new(memtable));
            }
        }

        state.memtable = {
            let id = memtables
                .iter()
                .chain(sst_ids.iter())
                .max()
                .copied()
                .unwrap_or(0)
                + 1;

            if options.enable_wal {
                let mt = Arc::new(MemTable::create_with_wal(
                    id,
                    Self::path_of_wal_static(path, id),
                )?);
                File::open(path)?.sync_all()?;

                manifest.add_record_when_init(ManifestRecord::NewMemtable(id))?;

                mt
            } else {
                Arc::new(MemTable::create(id))
            }
        };

        let next_sst_id = AtomicUsize::new(state.memtable.id() + 1);

        let initial_ts = std::iter::once(&state.memtable)
            .chain(state.imm_memtables.iter())
            .map(|mt| mt.max_ts())
            .chain(
                state
                    .get_all_sst_ids()
                    .iter()
                    .map(|id| state.sstables.get(id).unwrap().max_ts()),
            )
            .max()
            .unwrap_or(0)
            + 1;

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id,
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(initial_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.clean_up_unreferenced_ssts()?;
        storage.sync_dir()?;
        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub(crate) fn get_as_of(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        let state = { self.state.read().clone() };

        let key_begin = key_begin(key);
        let key_end = key_end(key);

        let mem_tables_itr = MergeIterator::create(
            std::iter::once(&state.memtable)
                .chain(state.imm_memtables.iter())
                .map(|mt: &Arc<MemTable>| Box::new(mt.clone().scan(key_begin, key_end)))
                .collect(),
        );

        let key_fp = farmhash::fingerprint32(key);

        let l0_itr = {
            let mut l0_itrs = Vec::with_capacity(state.l0_sstables.len());
            for sst in state
                .l0_sstables
                .iter()
                .map(|id| state.sstables.get(id).unwrap().clone())
                .filter(|sst| sst_can_contain_key(sst, key, key_fp))
            {
                let itr: SsTableIterator = SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?;
                l0_itrs.push(Box::new(itr));
            }
            MergeIterator::create(l0_itrs)
        };

        let levels_itr = {
            let mut level_itrs = Vec::with_capacity(state.levels.len());
            for (_, level_ssts) in &state.levels {
                // todo(ramneek): use binary search to find the sstable in the level...
                let ssts: Vec<Arc<SsTable>> = level_ssts
                    .iter()
                    .map(|id| state.sstables.get(id).unwrap().clone())
                    .filter(|sst| sst_can_contain_key(sst, key, key_fp))
                    .collect();
                if ssts.is_empty() {
                    continue;
                }
                let itr = SstConcatIterator::create_and_seek_to_key(
                    ssts,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?;
                level_itrs.push(Box::new(itr));
            }
            MergeIterator::create(level_itrs)
        };

        let itr = TwoMergeIterator::create(mem_tables_itr, l0_itr)?;
        let itr = TwoMergeIterator::create(itr, levels_itr)?;
        let itr = LsmIterator::new(itr, Bound::Unbounded, ts).map(FusedIterator::new)?;

        if itr.is_valid() && itr.key() == key {
            let val = itr.value();
            return if val.is_empty() {
                Ok(None)
            } else {
                Ok(Some(Bytes::copy_from_slice(val)))
            };
        }
        Ok(None)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(key)
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

    #[inline(always)]
    fn put_batch_inner(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let state = self.state.read();
        state.memtable.put_batch(data)?;
        let sz = state.memtable.approximate_size();
        drop(state);
        self.try_freeze(sz)
    }

    /// Write a batch of data into the storage.
    #[inline(always)]
    pub fn write_batch_inner<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<u64> {
        let _write_lock = self.mvcc.as_ref().unwrap().write_lock.lock();
        let ts = self.mvcc.as_ref().unwrap().latest_commit_ts() + 1;

        let mut data: Vec<(KeySlice, &[u8])> = Vec::with_capacity(batch.len());

        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    data.push((KeySlice::from_slice(key.as_ref(), ts), &[]));
                }
                WriteBatchRecord::Put(key, value) => {
                    let key = KeySlice::from_slice(key.as_ref(), ts);
                    let value = value.as_ref();
                    assert!(!value.is_empty());
                    data.push((key, value));
                }
            }
        }
        self.put_batch_inner(data.as_slice())?;

        self.mvcc.as_ref().unwrap().update_commit_ts(ts);
        Ok(ts)
    }

    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if self.options.serializable {
            // go through the txn API so that our write set in noted.
            let txn = self.new_txn()?;
            for record in batch {
                match record {
                    WriteBatchRecord::Del(key) => {
                        txn.delete(key.as_ref());
                    }
                    WriteBatchRecord::Put(key, value) => {
                        txn.put(key.as_ref(), value.as_ref());
                    }
                }
            }
            txn.commit()?
        } else {
            self.write_batch_inner(batch)?;
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
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
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let next_id = self.next_sst_id();
        let next = if self.options.enable_wal {
            let mem_table = MemTable::create_with_wal(next_id, self.path_of_wal(next_id))?;
            self.sync_dir()?;
            // First create and persist wal so the manifest doesn't have a dangling ref.
            // Next, record the memtable's wal in the manifest before exposing the memtable
            // to ensure we don't lose the writes synced to the memtable's wal.
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(state_lock_observer, ManifestRecord::NewMemtable(next_id))?;
            mem_table
        } else {
            MemTable::create(self.next_sst_id())
        };
        let next = Arc::new(next);

        {
            // flush the wal before exposing the new memtable so that
            // the system's sync() guarantees everything written before is persisted
            self.state.read().memtable.sync_wal()?;
        }
        {
            let mut sp = self.state.write();
            let mut new_state = sp.as_ref().clone();
            new_state.imm_memtables.insert(0, sp.memtable.clone());
            new_state.memtable = next;
            *sp = Arc::new(new_state);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_lock = self.state_lock.lock();

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

        if self.compaction_controller.flush_to_l0() {
            new_state.l0_sstables.insert(0, sst.sst_id());
        } else {
            new_state
                .levels
                .insert(0, (sst.sst_id(), vec![sst.sst_id()]))
        }

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&state_lock, ManifestRecord::Flush(sst.sst_id()))?
        }

        new_state.sstables.insert(sst.sst_id(), Arc::new(sst));
        {
            *self.state.write() = Arc::new(new_state);
        }
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self
            .mvcc
            .as_ref()
            .unwrap()
            .new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub(crate) fn scan_as_of(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = { self.state.read().clone() };

        let mem_tables_itr = MergeIterator::create(
            std::iter::once(&state.memtable)
                .chain(state.imm_memtables.iter())
                .map(|mt: &Arc<MemTable>| {
                    Box::new(mt.clone().scan(
                        map_key_ref_ts_bound(lower, TS_RANGE_BEGIN),
                        map_key_ref_ts_bound(upper, TS_RANGE_END),
                    ))
                })
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
                    Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                        ssts,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut itr = SstConcatIterator::create_and_seek_to_key(
                            ssts,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;
                        while itr.is_valid() && itr.key().key_ref() == key {
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
        LsmIterator::new(itr, upper, ts).map(FusedIterator::new)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(lower, upper)
    }
}

fn key_begin(key: &[u8]) -> Bound<KeySlice> {
    Bound::Included(KeySlice::from_slice(key, TS_RANGE_BEGIN))
}

fn key_end(key: &[u8]) -> Bound<KeySlice> {
    Bound::Included(KeySlice::from_slice(key, TS_RANGE_END))
}

fn sst_can_contain_key(sst: &Arc<SsTable>, key: &[u8], key_fp: u32) -> bool {
    (sst.first_key().key_ref() <= key && key <= sst.last_key().key_ref())
        && (sst.bloom.as_ref().is_none_or(|b| b.may_contain(key_fp)))
}

fn sst_overlaps_range(sst: &Arc<SsTable>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    // intersection with Lower Bound -> inf
    match lower {
        Bound::Included(skey) if skey > sst.last_key().key_ref() => return false,
        Bound::Excluded(skey) if skey >= sst.last_key().key_ref() => return false,
        _ => {}
    }

    // intersection with -inf -> upper bound
    match upper {
        Bound::Included(ekey) if ekey < sst.first_key().key_ref() => return false,
        Bound::Excluded(ekey) if ekey <= sst.first_key().key_ref() => return false,
        _ => {}
    }

    true
}

fn sst_seek_to_start(sst: Arc<SsTable>, start: Bound<&[u8]>) -> Result<SsTableIterator> {
    match start {
        Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst),
        Bound::Included(skey) if skey == sst.first_key().key_ref() => {
            SsTableIterator::create_and_seek_to_first(sst)
        }
        Bound::Included(skey) => {
            SsTableIterator::create_and_seek_to_key(sst, KeySlice::from_slice(skey, TS_RANGE_BEGIN))
        }
        Bound::Excluded(skey) => {
            let mut itr = SsTableIterator::create_and_seek_to_key(
                sst,
                KeySlice::from_slice(skey, TS_RANGE_BEGIN),
            )?;
            while itr.is_valid() && itr.key().key_ref() == skey {
                itr.next()?;
            }
            Ok(itr)
        }
    }
}
