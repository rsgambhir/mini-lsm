use anyhow::{bail, Result};

use crate::{
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = MergeIterator<MemTableIterator>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
}

impl LsmIterator {
    pub(crate) fn new(mut iter: LsmIteratorInner) -> Result<Self> {
        Self::remove_front_del_markers(&mut iter)?;
        Ok(Self { inner: iter })
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
        self.inner.key().raw_ref()
    }

    fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        Self::remove_front_del_markers(&mut self.inner)
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
}
