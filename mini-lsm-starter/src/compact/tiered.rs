use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let mut engine_size = 0;
        snapshot
            .levels
            .iter()
            .take(snapshot.levels.len() - 1)
            .for_each(|(_, ssts)| engine_size += ssts.len());

        let space_amp =
            (engine_size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        // Trigger a full compaction if estimated space amp is large.
        if space_amp >= self.options.max_size_amplification_percent as f64 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let max_tiers_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));

        let mut size = 0.0;
        for i in 0..max_tiers_to_take - 1 {
            size += snapshot.levels[i].1.len() as f64;
            let next_size = snapshot.levels[i + 1].1.len() as f64;
            if (next_size / size) <= size_ratio_trigger {
                // take the next tier as well
                continue;
            }
            // take 0...i
            let to_take = i + 1;
            if to_take < self.options.min_merge_width {
                continue;
            }
            // loop condition ensures to_take <= max_tiers_to_take - 1
            return Some(TieredCompactionTask {
                tiers: snapshot
                    .levels
                    .iter()
                    .take(to_take)
                    .cloned()
                    .collect::<Vec<_>>(),
                bottom_tier_included: to_take == snapshot.levels.len(),
            });
        }

        // unconditionally compact max_tier_width
        let to_take = max_tiers_to_take;
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: to_take >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut tiers: &[_] = &task.tiers;
        assert!(!tiers.is_empty());

        let mut new_levels = Vec::with_capacity(snapshot.levels.len() - tiers.len() + 1);
        let mut del = Vec::with_capacity(task.tiers.iter().fold(0, |acc, e| acc + e.1.len()));

        for (tier_id, tier_ssts) in snapshot.levels {
            if !tiers.is_empty() && tiers[0].0 == tier_id {
                del.extend_from_slice(&tier_ssts);
                tiers = &tiers[1..];
                if tiers.is_empty() {
                    new_levels.push((output[0], output.to_vec()))
                }
            } else {
                new_levels.push((tier_id, tier_ssts))
            }
        }

        snapshot.levels = new_levels;

        (snapshot, del)
    }
}
