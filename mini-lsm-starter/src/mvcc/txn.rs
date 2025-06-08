use anyhow::{bail, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{atomic::AtomicBool, Arc},
};

use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::WriteBatchRecord;
use crate::mem_table::map_bound;
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::StorageIterator,
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    fn check_commited(&self) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("use of commited txn")
        }
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(write_read_set) = &self.key_hashes {
            let fp = farmhash::fingerprint32(key);
            write_read_set.lock().1.insert(fp);
        }
    }
    fn add_to_write_set(&self, key: &[u8]) {
        if let Some(write_read_set) = &self.key_hashes {
            let fp = farmhash::fingerprint32(key);
            write_read_set.lock().0.insert(fp);
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_commited();
        self.add_to_read_set(key);
        let val = self.local_storage.get(key).map(|e| e.value().clone());
        if let Some(val) = val {
            return if val.is_empty() {
                Ok(None)
            } else {
                Ok(Some(val))
            };
        };
        self.inner.get_as_of(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.check_commited();
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Default::default(), Default::default()),
        }
        .build();
        local_iter.next().expect("doesn't return error");
        let lsm_iter = self.inner.scan_as_of(lower, upper, self.read_ts)?;
        let iter = TwoMergeIterator::create(local_iter, lsm_iter)?;
        TxnIterator::create(self.clone(), iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.check_commited();
        self.add_to_write_set(key);
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        self.check_commited();
        self.add_to_write_set(key);
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(&[]));
    }

    pub fn commit(&self) -> Result<()> {
        if self
            .committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            panic!("commit after commit")
        }
        let mvcc = self.inner.mvcc();

        let _commit_lock = mvcc.commit_lock.lock();
        let expected_commit_ts = mvcc.latest_commit_ts() + 1;

        let serialize = self.inner.options.serializable;
        if serialize {
            let wr_set = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, read_set) = &*wr_set;
            // Read only tx can always be serialized at the txn's read_ts.
            if !write_set.is_empty() {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_, txn_data) in committed_txns.range((self.read_ts + 1)..expected_commit_ts) {
                    // We only need to check conflicts b/w our read set and commited(and possible conflicting)
                    // txns write sets. Blind writes can always be serialized. If a write depended
                    // on a something from the db, then the read set would capture it.
                    // The already commited txn's couldn't have observed our writes because we haven't commited yet.
                    if txn_data.key_hashes.intersection(read_set).next().is_some() {
                        bail!("OCC: unable to serialize txn, please retry");
                    }
                }
            }
        }

        // write the data to the storage engine, the storage engine will add records
        // with version expected_commit_ts.
        let batch = self
            .local_storage
            .iter()
            .map(|e| {
                if !e.value().is_empty() {
                    WriteBatchRecord::Put(e.key().clone(), e.value().clone())
                } else {
                    WriteBatchRecord::Del(e.value().clone())
                }
            })
            .collect::<Vec<WriteBatchRecord<_>>>();

        let commit_ts = self.inner.write_batch_inner(&batch)?;
        assert_eq!(expected_commit_ts, commit_ts);

        // before giving up the commit lock, add our txns write set
        if serialize {
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            let mut wr_set = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _) = &mut *wr_set;

            committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );
            // cleanup
            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = committed_txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner
            .mvcc
            .as_ref()
            .unwrap()
            .ts
            .lock()
            .1
            .remove_reader(self.read_ts)
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_val = self.with_iter_mut(|iter| {
            iter.next()
                .map(|e| (e.key().clone(), e.value().clone()))
                .unwrap_or((Default::default(), Default::default()))
        });
        self.with_item_mut(|x| *x = next_val);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter };
        iter.move_to_next_key()?;
        Ok(iter)
    }

    fn move_to_next_key(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        if self.iter.is_valid() {
            self.txn.as_ref().add_to_read_set(self.iter.key())
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.iter.next()?;
            self.move_to_next_key()?
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
