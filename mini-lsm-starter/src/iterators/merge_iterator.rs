use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use crate::key::KeySlice;
use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    // invariant: iters and current are all is_valid
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut h = BinaryHeap::from(
            iters
                .into_iter()
                .enumerate()
                .filter_map(|(i, itr)| itr.is_valid().then(|| HeapWrapper(i, itr)))
                .collect::<Vec<HeapWrapper<I>>>(),
        );
        let c = h.pop();
        MergeIterator {
            iters: h,
            current: c,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some()
    }

    fn next(&mut self) -> Result<()> {
        let mut current =
            std::mem::take(&mut self.current).expect("next called on an invalid merge iterator");
        let curr_key = current.1.key();

        loop {
            let Some(top_peek) = self.iters.peek_mut() else {
                break;
            };
            if top_peek.1.key().ne(&curr_key) {
                break;
            }
            let mut top = PeekMut::pop(top_peek);
            top.1.next()?;
            if top.1.is_valid() {
                self.iters.push(top);
            }
        }

        current.1.next()?;
        if current.1.is_valid() {
            self.iters.push(current);
        }

        self.current = self.iters.pop();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len() + 1
    }
}
