use std::sync::Arc;

use super::SsTable;
use crate::block::Block;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};
use anyhow::Result;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator<const BYPASS_CACHE: bool = false> {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl<const BYPASS_CACHE: bool> SsTableIterator<BYPASS_CACHE> {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::_seek_to_key(table.as_ref(), KeySlice::default())?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (blk_idx, blk_iter) = Self::_seek_to_key(self.table.as_ref(), KeySlice::default())?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    fn _read_block(table: &SsTable, block_idx: usize) -> Result<Arc<Block>> {
        // todo: use MMAPed io to bypass OS cache.
        if BYPASS_CACHE {
            table.read_block(block_idx)
        } else {
            table.read_block_cached(block_idx)
        }
    }

    fn _seek_to_key(table: &SsTable, key: KeySlice) -> Result<(usize, BlockIterator)> {
        if key.is_empty() {
            Ok((
                0,
                BlockIterator::create_and_seek_to_first(Self::_read_block(table, 0)?),
            ))
        } else {
            let idx = table.find_block_idx(key);
            Ok((
                idx,
                BlockIterator::create_and_seek_to_key(Self::_read_block(table, idx)?, key),
            ))
        }
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::_seek_to_key(table.as_ref(), key)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    pub fn create_and_seek_to_partition_point<P>(_table: Arc<SsTable>, _pred: P) -> Self
    where
        P: FnMut(KeySlice) -> bool,
    {
        todo!("(ramneek) implement partition point API");
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::_seek_to_key(self.table.as_ref(), key)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    pub fn seek_to_partition_point<P>(&mut self, _pred: P)
    where
        P: FnMut(KeySlice) -> bool,
    {
        todo!("(ramneek) implement partition point API");
    }
}

impl<const BYPASS_CACHE: bool> StorageIterator for SsTableIterator<BYPASS_CACHE> {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        while !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx == self.table.num_of_blocks() {
                break;
            }
            self.blk_iter = BlockIterator::create_and_seek_to_first(Self::_read_block(
                &self.table,
                self.blk_idx,
            )?);
        }

        Ok(())
    }
}
