use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    target_block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        // todo(ramneek): prealloc vec?
        Self {
            offsets: vec![],
            data: vec![],
            target_block_size: block_size,
            first_key: Default::default(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if key.len() > u16::MAX as usize {
            panic!("key size too large")
        }
        if value.len() > u16::MAX as usize {
            panic!("value size too large")
        }
        // todo(ramneek): should we move num elements to the front?
        let new_size = /* KV data */ self.data.len() + 2 + key.len() + 2 + value.len() +
            /* offsets */ 2 * (self.offsets.len() + 1) + /* num elements */2;
        if !self.offsets.is_empty() && new_size > self.target_block_size {
            return false;
        }
        // note: overflow checks at the start
        let overlap = key
            .raw_ref()
            .iter()
            .zip(self.first_key.raw_ref().iter())
            .take_while(|(x, y)| x == y)
            .count() as u16;

        let key_len = key.len() as u16 - overlap;
        let val_len = value.len() as u16;

        // note: overflow checked at the last add
        self.offsets.push(self.data.len() as u16);

        self.data.extend_from_slice(&overlap.to_le_bytes());
        self.data.extend_from_slice(&key_len.to_le_bytes());
        self.data
            .extend_from_slice(&key.into_inner()[overlap as usize..]);

        self.data.extend_from_slice(&val_len.to_le_bytes());
        self.data.extend_from_slice(value);

        if self.data.len() > u16::MAX as usize {
            panic!("block data length too large")
        }

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
