use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use std::cmp;
use std::sync::Arc;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The first key in the block
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
    fn get_first_key(&self) -> KeyVec {
        let mut buf: &[u8] = &self.data[self.offsets[0] as usize..];
        buf.get_u16_le();
        let key_len = buf.get_u16_le() as usize;
        KeyVec::from_vec(buf[..key_len].to_vec())
    }
}

impl BlockIterator {
    // compares without materializing the key
    fn compare_offset_with_key(&self, offset: u16, given_key: KeySlice) -> cmp::Ordering {
        let mut buf: &[u8] = &self.block.data[offset as usize..];
        let overlap = buf.get_u16_le() as usize;
        let key_len = buf.get_u16_le() as usize;
        let key_after_overlap: &[u8] = &buf[..key_len];
        let key_itr = self
            .first_key
            .as_key_slice()
            .raw_ref()
            .iter()
            .take(overlap)
            .chain(key_after_overlap.iter());

        key_itr.cmp(given_key.raw_ref().iter())
    }

    fn get_kv_at_idx(&self, i: usize) -> (KeyVec, ValueRange) {
        let key_off = self.block.offsets[i] as usize;
        let mut buf: &[u8] = &self.block.data[key_off..];
        let overlap = buf.get_u16_le() as usize;
        let key_len = buf.get_u16_le() as usize;
        let key_after_overlap: &[u8] = &buf[..key_len];
        let key: Vec<u8> = self
            .first_key
            .as_key_slice()
            .raw_ref()
            .iter()
            .take(overlap)
            .chain(key_after_overlap.iter())
            .cloned()
            .collect();

        let val_off = key_off + 4 + key_len;
        let val_len = (&self.block.data[val_off..]).get_u16_le() as usize;

        (KeyVec::from_vec(key), (val_off + 2, val_off + 2 + val_len))
    }

    fn get_val(&self, vr: ValueRange) -> &[u8] {
        &self.block.data.as_slice()[vr.0..vr.1]
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let first_key = block.get_first_key();
        Self {
            block,
            first_key,
            key: KeyVec::new(), // is_valid == false
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        assert!(!block.offsets.is_empty());
        let mut itr = Self::new(block);
        itr.seek_to_first();
        itr
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
        self.get_val(self.value_range)
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
                .partition_point(|i| self.compare_offset_with_key(*i, key).is_lt()),
        );
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            // make invalid
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            self.idx = self.block.offsets.len();
        } else {
            let (k, vr) = self.get_kv_at_idx(idx);
            self.key = k;
            self.value_range = vr;
            self.idx = idx;
        }
    }
}
