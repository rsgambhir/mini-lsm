use std::path::Path;
use std::sync::Arc;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyVec;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use bytes::BufMut;

const BLOOM_FILTER_FPR: f64 = 0.01;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    max_ts: u64,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_fingerprints: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::default(),
            last_key: KeyVec::default(),
            data: vec![],
            meta: vec![],
            block_size,
            key_fingerprints: vec![],
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.split_new_block();

            if !self.builder.add(key, value) {
                panic!("new block builder didn't accept the first key")
            }
        }

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }
        self.last_key = key.to_key_vec();
        self.max_ts = self.max_ts.max(key.ts());
        self.key_fingerprints
            .push(farmhash::fingerprint32(key.key_ref()));
    }

    pub fn last_added_key(&self) -> &[u8] {
        self.last_key.key_ref()
    }

    fn split_new_block(&mut self) {
        if self.first_key.is_empty() {
            return;
        }
        let block =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size)).build();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: std::mem::take(&mut self.first_key).into_key_bytes(),
            last_key: std::mem::take(&mut self.last_key).into_key_bytes(),
        });
        self.data.extend(block.encode());
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.split_new_block();

        if self.meta.is_empty() {
            panic!("building empty sstable")
        }

        let meta_off = self.data.len();
        if meta_off > u32::MAX as usize {
            panic!("SSTable too big")
        }
        BlockMeta::encode_block_meta(self.meta.as_slice(), &mut self.data);
        self.data.put_u32_le(meta_off as u32);
        self.data.put_u64_le(self.max_ts);

        let bf = Bloom::build_from_key_hashes(
            self.key_fingerprints.as_slice(),
            Bloom::bloom_bits_per_key(self.key_fingerprints.len(), BLOOM_FILTER_FPR),
        );
        let bf_offset = self.data.len();
        if meta_off > u32::MAX as usize {
            panic!("SSTable too big")
        }
        bf.encode(&mut self.data);
        self.data.put_u32_le(bf_offset as u32);

        Ok(SsTable {
            file: FileObject::create(path.as_ref(), self.data)?,
            block_meta_offset: meta_off,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: Some(bf),
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
