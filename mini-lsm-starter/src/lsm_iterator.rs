use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::mem_table::map_bound;
use crate::table::SsTableIterator;
use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};
use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
}

fn key_in_end_bound(key: &[u8], end_bound: &Bound<Bytes>) -> bool {
    match end_bound {
        Bound::Included(ekey) => key <= ekey,
        Bound::Excluded(ekey) => key < ekey,
        Bound::Unbounded => true,
    }
}

impl LsmIterator {
    pub(crate) fn new(mut iter: LsmIteratorInner, end_bound: Bound<&[u8]>) -> Result<Self> {
        Self::remove_front_del_markers(&mut iter)?;
        Ok(Self {
            inner: iter,
            end_bound: map_bound(end_bound),
        })
    }

    fn remove_front_del_markers(itr: &mut LsmIteratorInner) -> Result<()> {
        while itr.is_valid() && itr.value().is_empty() {
            itr.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid() && key_in_end_bound(self.inner.key().key_ref(), &self.end_bound)
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        Self::remove_front_del_markers(&mut self.inner)
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("use after invalid");
        }
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("use after invalid");
        }
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("next used after iterator returned error");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
