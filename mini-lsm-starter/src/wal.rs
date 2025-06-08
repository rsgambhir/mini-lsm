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
        // todo(ramneek): allow recovery from crashes, make WAL writing more robust to crashes
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;

        // todo(ramneek): this can be large, use buffered reader over the file
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();

        while buf.has_remaining() {
            let batch_len = buf.get_u64_le() as usize;
            if buf.remaining() < batch_len {
                panic!("incomplete WAL")
            }
            let mut batch = &buf[..batch_len];
            while batch.has_remaining() {
                let key = {
                    let key_len = batch.get_u16_le() as usize;
                    let key = batch.copy_to_bytes(key_len);
                    let key_ts = batch.get_u64_le();
                    KeyBytes::from_bytes_with_ts(key, key_ts)
                };
                let val = {
                    let val_len = batch.get_u16_le() as usize;
                    batch.copy_to_bytes(val_len)
                };
                skiplist.insert(key, val);
            }
            buf.advance(batch_len)
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::new();
        for (key, value) in data {
            let key_len = key.key_len().try_into()?;
            let val_len = value.len().try_into()?;
            // todo: define a global source of truth for max key and value len
            buf.put_u16_le(key_len);
            buf.put_slice(key.key_ref());
            buf.put_u64_le(key.ts());

            buf.put_u16_le(val_len);
            buf.put_slice(value);
        }
        file.write_all(&(buf.len() as u64).to_le_bytes())?;
        file.write_all(&buf)?;
        // todo: checksum
        Ok(())
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
