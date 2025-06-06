use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        // todo(ramneek): this can be large, use buffered reader over the file
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf = buf.as_slice();
        while buf.has_remaining() {
            let key_len = buf.get_u16_le() as usize;
            let key = Bytes::copy_from_slice(&buf[..key_len]);
            buf.advance(key_len);
            let val_len = buf.get_u16_le() as usize;
            let val = Bytes::copy_from_slice(&buf[..val_len]);
            buf.advance(val_len);
            skiplist.insert(key, val);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_len = key.len().try_into()?;
        let val_len = value.len().try_into()?;
        let mut buf = Vec::with_capacity((2 + key_len + 2 + val_len) as usize);
        // todo: define a global source of truth for max key and value len
        buf.put_u16_le(key_len);
        buf.put_slice(key);
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
