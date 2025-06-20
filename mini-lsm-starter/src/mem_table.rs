use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;
use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use ouroboros::self_referencing;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_key_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn map_key_ref_ts_bound(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, ts)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        MemTable {
            map: Arc::new(Default::default()),
            wal: None,
            id,
            approximate_size: Arc::new(Default::default()),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        Ok(MemTable {
            map: Arc::new(Default::default()),
            wal: Some(Wal::create(path)?),
            id,
            approximate_size: Arc::new(Default::default()),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let map = SkipMap::default();
        let wal = Wal::recover(path, &map)?;

        Ok(MemTable {
            map: Arc::new(map),
            wal: Some(wal),
            id,
            approximate_size: Arc::new(Default::default()),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            map_key_ref_ts_bound(lower, TS_DEFAULT),
            map_key_ref_ts_bound(upper, TS_DEFAULT),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let key = KeyBytes::from_bytes_with_ts(
            Bytes::from_static(unsafe { std::mem::transmute::<&[u8], &[u8]>(key.key_ref()) }),
            key.ts(),
        );
        self.map.get(&key).map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut batch_size = 0;
        for (key, value) in data {
            let key_len = key.key_len();
            let val_len = value.len();
            if key_len > u16::MAX as usize {
                bail!("key len {} too large", key_len);
            }
            if val_len > u16::MAX as usize {
                bail!("val len {} too large", val_len);
            }
            if key_len == 0 {
                panic!("empty key put")
            }
            self.map.insert(
                key.to_key_vec().into_key_bytes(),
                Bytes::copy_from_slice(value),
            );
            batch_size += key.raw_len() + value.len();
        }

        self.approximate_size.fetch_add(batch_size, Relaxed);

        if let Some(wal) = &self.wal {
            wal.put_batch(data)?
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |map| map.range((map_key_bound(lower), map_key_bound(upper))),
            item: MemTableIterator::empty_entry(),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        self.map
            .iter()
            .for_each(|e| builder.add(e.key().as_key_slice(), e.value().as_bytes()));
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn max_ts(&self) -> u64 {
        self.map.iter().map(|e| e.key().ts()).max().unwrap_or(0)
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn empty_entry() -> (KeyBytes, Bytes) {
        (KeyBytes::default(), Bytes::from_static(&[]))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_bytes()
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_val = self.with_iter_mut(|iter| {
            iter.next()
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or(MemTableIterator::empty_entry())
        });
        self.with_item_mut(|x| *x = next_val);
        Ok(())
    }
}
