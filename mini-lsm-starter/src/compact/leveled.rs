use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;
use crate::util::conversion::to_hashset;
use itertools::Itertools;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().first_key())
            .min()
            .cloned()
            .unwrap();

        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables.get(id).unwrap().last_key())
            .max()
            .cloned()
            .unwrap();

        snapshot.levels[level - 1]
            .1
            .iter()
            .filter(|sst_id| {
                let sst = snapshot.sstables.get(sst_id).unwrap();
                !(*sst.last_key() < begin_key || sst.first_key() > &end_key)
            })
            .copied()
            .collect()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        assert_eq!(snapshot.levels.len(), self.options.max_levels);

        let base_level_size = self.options.base_level_size_mb * 1024 * 1024;

        let level_sizes = snapshot
            .levels
            .iter()
            .map(|(_, level_ssts)| {
                level_ssts
                    .iter()
                    .map(|sst_id| snapshot.sstables.get(sst_id).unwrap().table_size())
                    .sum::<u64>() as usize
            })
            .collect::<Vec<usize>>();

        let mut base_level = self.options.max_levels;

        let mut target_level_sizes = vec![0; self.options.max_levels];
        target_level_sizes[base_level - 1] = level_sizes[base_level - 1].max(base_level_size);

        for level in (1..self.options.max_levels).rev() {
            let next_level_size = target_level_sizes[level + 1 - 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size {
                target_level_sizes[level - 1] = this_level_size;
            }
            if target_level_sizes[level - 1] > 0 {
                base_level = level
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: Self::find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let level_priority =
            |level| (level_sizes[level - 1] as f64) / (target_level_sizes[level - 1] as f64);

        (1..self.options.max_levels)
            .filter(|level| level_sizes[level - 1] > target_level_sizes[level - 1])
            // note: inf == inf, so assert!(inf.partial_cmp(inf).is_some()), NaN filtered out above
            .max_by(|x, y| level_priority(*x).partial_cmp(&level_priority(*y)).unwrap())
            .map(|level_to_compact| {
                // take the oldest sst
                let selected_sst = *snapshot.levels[level_to_compact - 1]
                    .1
                    .iter()
                    .min()
                    .unwrap();
                LeveledCompactionTask {
                    upper_level: Some(level_to_compact),
                    upper_level_sst_ids: vec![selected_sst],
                    lower_level: level_to_compact + 1,
                    lower_level_sst_ids: Self::find_overlapping_ssts(
                        snapshot,
                        &[selected_sst],
                        level_to_compact + 1,
                    ),
                    is_lower_level_bottom_level: level_to_compact + 1 == self.options.max_levels,
                }
            })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let compacted_upper_level_ssts = to_hashset(task.upper_level_sst_ids.clone());
        let compacted_lower_level_ssts = to_hashset(task.lower_level_sst_ids.clone());

        let upper_ssts = if let Some(upper_level) = &task.upper_level {
            &mut snapshot.levels[upper_level - 1].1
        } else {
            &mut snapshot.l0_sstables
        };
        *upper_ssts = upper_ssts
            .iter()
            .filter(|id| !compacted_upper_level_ssts.contains(*id))
            .copied()
            .collect();

        let lower_ssts = &mut snapshot.levels[task.lower_level - 1].1;
        *lower_ssts = {
            let itr = lower_ssts
                .iter()
                .filter(|id| !compacted_lower_level_ssts.contains(*id))
                .chain(output.iter())
                .copied();

            if !in_recovery {
                itr.sorted_by_key(|id| snapshot.sstables.get(id).unwrap().first_key())
                    .collect()
            } else {
                itr.collect()
            }
        };

        let del: Vec<usize> = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .chain(task.lower_level_sst_ids.iter().copied())
            .collect();
        (snapshot, del)
    }
}
