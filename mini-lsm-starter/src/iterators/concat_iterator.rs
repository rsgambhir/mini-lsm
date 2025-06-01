use std::sync::Arc;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};
use anyhow::Result;

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator<true>>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let current = Some(SsTableIterator::create_and_seek_to_first(
            sstables[0].clone(),
        )?);

        Ok(Self {
            current,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() || key.raw_ref() > sstables.last().unwrap().last_key().raw_ref() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }

        let curr_idx = sstables.partition_point(|sst| key.raw_ref() > sst.last_key().raw_ref());

        let current = Some(SsTableIterator::create_and_seek_to_key(
            sstables[curr_idx].clone(),
            key,
        )?);

        Ok(Self {
            current,
            next_sst_idx: curr_idx + 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_pred<P>(_sstables: Vec<Arc<SsTable>>, _pred: P) -> Self
    where
        P: FnMut(&Arc<SsTable>) -> bool,
    {
        todo!("(ramneek) implement partition point API");
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let mut current = self.current.take().unwrap();
        current.next()?;
        if current.is_valid() {
            self.current.replace(current);
        } else if self.next_sst_idx < self.sstables.len() {
            self.current = Some(SsTableIterator::<true>::create_and_seek_to_first(
                self.sstables[self.next_sst_idx].clone(),
            )?);
            self.next_sst_idx += 1;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.sstables.len() - self.next_sst_idx + 1
    }
}
