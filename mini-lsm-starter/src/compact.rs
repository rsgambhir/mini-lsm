mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::Key;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn get_concat_itr(
        ssts: &HashMap<usize, Arc<SsTable>>,
        level_sst_ids: &[usize],
    ) -> Result<SstConcatIterator<true>> {
        SstConcatIterator::create_and_seek_to_first(
            level_sst_ids
                .iter()
                .map(|id| ssts.get(id).unwrap().clone())
                .collect(),
        )
    }

    fn get_l0_itr(
        state: &Arc<LsmStorageState>,
        l0_sst_ids: &[usize],
    ) -> Result<MergeIterator<SsTableIterator<true>>> {
        let mut l0_itrs: Vec<Box<SsTableIterator<true>>> = Vec::new();
        for sst in l0_sst_ids.iter().map(|id| state.sstables.get(id).unwrap()) {
            l0_itrs.push(Box::new(SsTableIterator::<true>::create_and_seek_to_first(
                sst.clone(),
            )?));
        }
        Ok(MergeIterator::create(l0_itrs))
    }

    /// itr MUST contain all the versions of a key in the levels that are to be compacted.
    fn build_sorted_run<Itr: for<'a> StorageIterator<KeyType<'a> = Key<&'a [u8]>>>(
        &self,
        mut itr: Itr,
        is_last_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        // todo(ramneek): remove dangling ssts here, or in a background thread.
        let watermark = self.mvcc.as_ref().unwrap().watermark();

        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut level_ssts: Vec<Arc<SsTable>> = Vec::new();

        while itr.is_valid() {
            let key = itr.key().to_key_vec();
            let mut latest_before_watermark_seen = false;
            // Add all the relevant versions for this key
            while itr.is_valid() {
                let next_key = itr.key();
                if next_key.key_ref() != key.key_ref() {
                    break;
                }
                if latest_before_watermark_seen {
                    // GC
                } else {
                    let val = itr.value();
                    let ts = next_key.ts();
                    latest_before_watermark_seen = ts <= watermark;
                    if is_last_level && val.is_empty() && ts <= watermark {
                        // GC the del marker in last level?
                        // Invariants:
                        //  1. The itr contains sstables from consecutive levels
                        //  2. Itr will contain _all_ the versions of the key in the levels to be compacted
                        //     - This is trivially true for the lower level.
                        //     - For the upper level, either the full level is taken(L0),
                        //       or if only one sst is taken for a non-L0, then it is guaranteed to
                        //       contain all the versions of the key since we are not splitting the
                        //       sst in compaction util we get a new key below.
                        //  This means that we can safely GC the del marker(and everything before it)
                        //  if it has ts <= watermark.
                        //      Note: can also GC the del marker if there is no version before it.
                        //            not doing it here for now for simplicity.
                        //
                        //  This would not be true if Itr doesn't contain all the versions of the key in the levels
                        //  to be compacted. GCing the del marker will expose some other version that is not in the
                        //  Itr but is in the levels.
                        //
                    } else {
                        builder.add(next_key, val);
                    }
                }
                itr.next()?
            }

            let should_end_sst = {
                if itr.is_valid() {
                    builder.estimated_size() > self.options.target_sst_size
                } else {
                    !builder.is_empty() // reached end
                }
            };
            if should_end_sst {
                let b =
                    std::mem::replace(&mut builder, SsTableBuilder::new(self.options.block_size));
                let id = self.next_sst_id();
                let new_sst = b.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                level_ssts.push(Arc::new(new_sst));
            }
        }

        Ok(level_ssts)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables: l0,
                l1_sstables: l1,
            } => {
                let state = { self.state.read().clone() };

                let l0_itr = Self::get_l0_itr(&state, l0)?;
                let l1_itr = Self::get_concat_itr(&state.sstables, l1)?;

                let itr = TwoMergeIterator::create(l0_itr, l1_itr)?;
                self.build_sorted_run(itr, task.compact_to_bottom_level())
            }

            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                let state = { self.state.read().clone() };

                let lower_itr = Self::get_concat_itr(&state.sstables, lower_level_sst_ids)?;
                if upper_level.is_some() {
                    // concat iterator
                    let upper_itr = Self::get_concat_itr(&state.sstables, upper_level_sst_ids)?;
                    let itr = TwoMergeIterator::create(upper_itr, lower_itr)?;
                    self.build_sorted_run(itr, task.compact_to_bottom_level())
                } else {
                    // merge iterator
                    let upper_itr = Self::get_l0_itr(&state, upper_level_sst_ids)?;
                    let itr = TwoMergeIterator::create(upper_itr, lower_itr)?;
                    self.build_sorted_run(itr, task.compact_to_bottom_level())
                }
            }

            CompactionTask::Tiered(task) => {
                let state = { self.state.read().clone() };

                let mut tiers_itrs = Vec::with_capacity(task.tiers.len());
                for (_, tier_sst_ids) in &task.tiers {
                    tiers_itrs.push(Box::new(Self::get_concat_itr(
                        &state.sstables,
                        tier_sst_ids,
                    )?));
                }
                let itr = MergeIterator::create(tiers_itrs);
                self.build_sorted_run(itr, task.bottom_tier_included)
            }
        }
    }

    fn cleanup_ssts(&self, ids: &[usize]) -> Result<()> {
        // NB: SSTable has an open file handle to the sst file,
        // any existing readers that still have a reference to the SST will prevent the OS from actually
        // deleting the file, if this facility is not available us, then we need to implement this functionality
        // in the deref trait of the SSTable or let the sstables be cleaned by a background thread.
        for id in ids {
            fs::remove_file(self.path_of_sst(*id))?
        }
        self.sync_dir()
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let old_state = { self.state.read().clone() };

        let old_l0_ssts = old_state.l0_sstables.clone();
        let old_l1_ssts = old_state.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: old_l0_ssts.clone(),
            l1_sstables: old_l1_ssts.clone(),
        };
        let new_ssts = self.compact(&task)?;

        {
            let state_lock = self.state_lock.lock();
            *self.state.write() = {
                let mut new_state = { self.state.read().as_ref().clone() };

                new_ssts
                    .iter()
                    .for_each(|x| _ = new_state.sstables.insert(x.sst_id(), x.clone()));

                new_state.levels[0].1 = new_ssts.iter().map(|x| x.sst_id()).collect();

                new_state.l0_sstables = new_state
                    .l0_sstables
                    .iter()
                    .take_while(|id| **id != old_l0_ssts[0])
                    .cloned()
                    .collect();

                old_l0_ssts
                    .iter()
                    .chain(old_l1_ssts.iter())
                    .for_each(|x| _ = new_state.sstables.remove(x));

                if let Some(manifest) = &self.manifest {
                    manifest.add_record(
                        &state_lock,
                        ManifestRecord::Compaction(task, new_state.levels[0].1.clone()),
                    )?
                }
                Arc::new(new_state)
            };
        }

        self.cleanup_ssts(
            &(old_l0_ssts
                .into_iter()
                .chain(old_l1_ssts)
                .collect::<Vec<usize>>()),
        )
    }

    fn trigger_compaction(&self) -> Result<()> {
        let state = { self.state.read().clone() };
        let task = self.compaction_controller.generate_compaction_task(&state);

        if task.is_none() {
            return Ok(());
        }
        let task = task.unwrap();

        println!("running compaction task: {:?}", task);
        println!("state before compaction...");
        self.dump_structure();

        let compacted_ssts = self.compact(&task)?;

        let state_lock = self.state_lock.lock();
        let mut state = { self.state.read().clone() }.as_ref().clone();

        compacted_ssts
            .iter()
            .for_each(|sst| _ = state.sstables.insert(sst.sst_id(), sst.clone()));

        let compaction_output = compacted_ssts
            .iter()
            .map(|sst| sst.sst_id())
            .collect::<Vec<usize>>();

        let (mut state, ssts_to_del) = self.compaction_controller.apply_compaction_result(
            &state,
            &task,
            &compaction_output,
            false,
        );

        if let Some(manifest) = &self.manifest {
            manifest.add_record(
                &state_lock,
                ManifestRecord::Compaction(task, compaction_output),
            )?
        }

        ssts_to_del
            .iter()
            .for_each(|sst_id| _ = state.sstables.remove(sst_id));

        {
            *self.state.write() = Arc::new(state);
        }

        drop(state_lock);

        println!("state after compaction...");
        self.dump_structure();

        self.cleanup_ssts(&ssts_to_del)
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                            panic!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush =
            { self.state.read().imm_memtables.len() + 1 > self.options.num_memtable_limit };

        if should_flush {
            self.force_flush_next_imm_memtable()
        } else {
            Ok(())
        }
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
