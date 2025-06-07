pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for bm in block_meta.iter() {
            buf.put_u32_le(bm.offset as u32);

            buf.put_u16_le(bm.first_key.key_len() as u16);
            buf.put_slice(bm.first_key.key_ref());
            buf.put_u64_le(bm.first_key.ts());

            buf.put_u16_le(bm.last_key.key_len() as u16);
            buf.put_slice(bm.last_key.key_ref());
            buf.put_u64_le(bm.last_key.ts());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = vec![];
        // todo add num blocks in the encoding?
        while buf.has_remaining() {
            let offset = buf.get_u32_le() as usize;

            let first_key = {
                let key_len = buf.get_u16_le() as usize;
                let key = buf.copy_to_bytes(key_len);
                let key_ts = buf.get_u64_le();
                KeyBytes::from_bytes_with_ts(key, key_ts)
            };

            let last_key = {
                let key_len = buf.get_u16_le() as usize;
                let key = buf.copy_to_bytes(key_len);
                let key_ts = buf.get_u64_le();
                KeyBytes::from_bytes_with_ts(key, key_ts)
            };

            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            })
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let mut curr_end = file.size();
        let bf_off = (&mut file.read(curr_end - 4, 4)?.as_slice()).get_u32_le() as u64;
        curr_end -= 4;
        let bf_size = curr_end - bf_off;
        let bf = Bloom::decode(file.read(bf_off, bf_size)?.as_slice())?;
        curr_end -= bf_size;

        let max_ts = file.read(curr_end - 8, 8)?.as_slice().get_u64_le();
        curr_end -= 8;

        let meta_off = (&mut file.read(curr_end - 4, 4)?.as_slice()).get_u32_le() as u64;
        curr_end -= 4;
        let meta_size = curr_end - meta_off;
        let block_meta = BlockMeta::decode_block_meta(file.read(meta_off, meta_size)?.as_slice());
        // curr_end -= meta_size;

        Ok(Self {
            file,
            block_meta_offset: meta_off as usize,
            id,
            block_cache,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            bloom: Some(bf),
            max_ts,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_start = self.block_meta[block_idx].offset as u64;
        let block_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |bm| bm.offset) as u64;
        Ok(Arc::new(Block::decode(
            self.file
                .read(block_start, block_end - block_start)?
                .as_slice(),
        )))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        let read_block = || self.read_block(block_idx);

        self.block_cache.as_ref().map_or(read_block(), |c| {
            c.try_get_with((self.id, block_idx), read_block)
                .map_err(|e| anyhow!("{}", e))
        })
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let idx = self
            .block_meta
            .partition_point(|bm| bm.last_key.as_key_slice() < key);
        if idx >= self.num_of_blocks() {
            self.num_of_blocks() - 1
        } else {
            idx
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
