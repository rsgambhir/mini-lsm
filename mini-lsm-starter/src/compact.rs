#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
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
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables: l0,
                l1_sstables: l1,
            } => {
                let state = { self.state.read().clone() };

                let mut l0_itrs: Vec<Box<SsTableIterator<true>>> = Vec::new();
                for sst in l0.iter().map(|id| state.sstables.get(id).unwrap()) {
                    l0_itrs.push(Box::new(SsTableIterator::<true>::create_and_seek_to_first(
                        sst.clone(),
                    )?));
                }
                let l0_itr = MergeIterator::create(l0_itrs);

                let l1_itr = SstConcatIterator::create_and_seek_to_first(
                    l1.iter()
                        .map(|id| state.sstables.get(id).unwrap().clone())
                        .collect(),
                )?;

                let mut itr = TwoMergeIterator::create(l0_itr, l1_itr)?;

                let mut new_l1_ssts: Vec<Arc<SsTable>> = Vec::new();

                let mut builder = SsTableBuilder::new(self.options.block_size);
                // todo(ramneek): remove dangling ssts here, or in a background thread.
                while itr.is_valid() {
                    let key = itr.key();
                    let val = itr.value();
                    if !val.is_empty() {
                        builder.add(key, val);
                    }
                    itr.next()?;
                    if (!itr.is_valid() && !builder.is_empty()) || // reached end
                        builder.estimated_size() > self.options.target_sst_size
                    {
                        let b = std::mem::replace(
                            &mut builder,
                            SsTableBuilder::new(self.options.block_size),
                        );
                        let id = self.next_sst_id();
                        let new_sst =
                            b.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                        new_l1_ssts.push(Arc::new(new_sst));
                    }
                }

                Ok(new_l1_ssts)
            }
            _ => panic!("not supported"),
        }
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

        *self.state.write() = {
            let _lock = self.state_lock.lock();
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

            Arc::new(new_state)
        };

        // clean up old files
        // note that SSTable has an open file handle to the sst file,
        // any old readers that still have a reference to the SST will prevent the OS from actually
        // deleting the file, if this facility is not available us, then we need to implement this functionality
        // in the deref trait of the SSTable or let the sstables be cleaned by a background thread.
        for id in old_l0_ssts.iter().chain(old_l1_ssts.iter()) {
            fs::remove_file(self.path_of_sst(*id))?
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
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
