use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};
use anyhow::Result;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(path)?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        // todo(ramneek): this can be large, use buffered reader over the file
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();
        while buf.has_remaining() {
            let key = {
                let key_len = buf.get_u16_le() as usize;
                let key = buf.copy_to_bytes(key_len);
                let key_ts = buf.get_u64_le();
                KeyBytes::from_bytes_with_ts(key, key_ts)
            };
            let val = {
                let val_len = buf.get_u16_le() as usize;
                buf.copy_to_bytes(val_len)
            };
            skiplist.insert(key, val);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        let key_len = key.key_len().try_into()?;
        let val_len = value.len().try_into()?;
        let mut buf = Vec::with_capacity((2 + key_len + 8 + 2 + val_len) as usize);
        // todo: define a global source of truth for max key and value len
        buf.put_u16_le(key_len);
        buf.put_slice(key.key_ref());
        buf.put_u64_le(key.ts());

        buf.put_u16_le(val_len);
        buf.put_slice(value);

        let mut file = self.file.lock();
        file.write_all(&buf)?;

        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        if !file.buffer().is_empty() {
            file.flush()?;
            file.get_mut().sync_all()?;
        }
        Ok(())
    }
}
