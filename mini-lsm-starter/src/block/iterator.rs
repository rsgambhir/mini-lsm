use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The first key in the block
    #[allow(dead_code)]
    first_key: KeyVec,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: ValueRange,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

type ValueRange = (usize, usize);

impl Block {
    // todo(ramneek): change return type to KeyVec?
    #[allow(dead_code)]
    fn get_key_at_idx(&self, i: usize) -> KeySlice {
        self.get_key_at_offset(self.offsets[i] as usize)
    }

    fn get_key_at_offset(&self, offset: usize) -> KeySlice {
        let key_len = (&self.data.as_slice()[offset..]).get_u16_le() as usize;
        KeySlice::from_slice(&self.data.as_slice()[offset + 2..offset + 2 + key_len])
    }

    fn get_kv_at_idx(&self, i: usize) -> (KeySlice, ValueRange) {
        let key_off = self.offsets[i] as usize;
        let key_len = (&self.data.as_slice()[key_off..]).get_u16_le() as usize;

        let key = KeySlice::from_slice(&self.data.as_slice()[key_off + 2..key_off + 2 + key_len]);
        let val_off = key_off + 2 + key_len;
        let val_len = (&self.data.as_slice()[val_off..]).get_u16_le() as usize;

        (key, (val_off + 2, val_off + 2 + val_len))
    }

    fn get_val(&self, vr: ValueRange) -> &[u8] {
        &self.data.as_slice()[vr.0..vr.1]
    }
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            first_key: KeyVec::new(),
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        if block.offsets.len() > 0 {
            let (k, value_range) = block.get_kv_at_idx(0);
            let key = k.to_key_vec();
            let first_key = k.to_key_vec();
            Self {
                block,
                first_key,
                key,
                value_range,
                idx: 0,
            }
        } else {
            Self::new(block)
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut itr = Self::new(block);
        itr.seek_to_key(key);
        itr
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.block.get_val(self.value_range)
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // todo(ramneek): note: documentation of partition_point doesn't say that the passed closure will only
        //  be used on elements from the Vec...
        self.seek_to_idx(
            self.block
                .offsets
                .partition_point(|i| self.block.get_key_at_offset(*i as usize) < key),
        );
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.idx = self.block.offsets.len();
        } else {
            let (k, vr) = self.block.get_kv_at_idx(idx);
            self.key = k.to_key_vec();
            self.value_range = vr;
            self.idx = idx;
        }
    }
}
