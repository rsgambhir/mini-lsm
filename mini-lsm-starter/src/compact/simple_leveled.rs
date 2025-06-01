use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: self.options.max_levels == 1,
            });
        }

        (1..self.options.max_levels) // level number
            .find(|level| {
                let level = level - 1; // level index
                let upper = snapshot.levels[level].1.len();
                let lower = snapshot.levels[level + 1].1.len();
                // (lower / upper) * 100 < size_ratio_percent
                // lower < size_ratio_percent * upper / 100
                (lower as f64) < self.options.size_ratio_percent as f64 / 100.0 * upper as f64
            })
            .map(|lno| SimpleLeveledCompactionTask {
                upper_level: Some(lno),
                upper_level_sst_ids: snapshot.levels[lno - 1].1.clone(),
                lower_level: lno + 1,
                lower_level_sst_ids: snapshot.levels[lno - 1 + 1].1.clone(),
                is_lower_level_bottom_level: lno + 1 == self.options.max_levels,
            })
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        println!("simple compaction: apply compaction result: {:?}", output);
        let mut snapshot = snapshot.clone();

        let curr_upper = if let Some(lno) = task.upper_level {
            &mut snapshot.levels[lno - 1].1
        } else {
            &mut snapshot.l0_sstables
        };

        // remove old ssts. since ssts are added in order,
        // we take everything before task.upper_level_sst_ids[0]
        *curr_upper = curr_upper
            .iter()
            .take_while(|id| **id != task.upper_level_sst_ids[0])
            .copied()
            .collect();

        assert_eq!(
            task.upper_level_sst_ids
                .iter()
                .collect::<HashSet<_>>()
                .intersection(&curr_upper.iter().collect())
                .collect::<Vec<_>>()
                .len(),
            0
        );

        snapshot.levels[task.lower_level - 1].1 = output.to_vec();

        println!(
            "lower level after compaction: L{}: {:?}",
            task.lower_level,
            snapshot.levels[task.lower_level - 1].1
        );

        let del = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .chain(task.lower_level_sst_ids.iter().copied())
            .collect();

        (snapshot, del)
    }
}
