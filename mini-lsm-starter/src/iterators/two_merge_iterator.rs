use anyhow::Result;
use std::cmp::Ordering;

use super::StorageIterator;

enum Current {
    A,
    B,
    None,
}

impl Current {
    fn is_none(&self) -> bool {
        matches!(self, Current::None)
    }
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    curr: Current,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, mut b: B) -> Result<Self> {
        let mut curr = Current::None;
        if !a.is_valid() {
            if b.is_valid() {
                curr = Current::B;
            }
            return Ok(Self { a, b, curr });
        }

        curr = Current::A;

        if b.is_valid() {
            let cmp = a.key().cmp(&b.key());
            match cmp {
                Ordering::Greater => curr = Current::B,
                Ordering::Equal => {
                    b.next()?;
                }
                _ => {}
            }
        }

        Ok(Self { a, b, curr })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn value(&self) -> &[u8] {
        match self.curr {
            Current::A => self.a.value(),
            Current::B => self.b.value(),
            _ => panic!("TwoMergeIterator: key called on invalid iterator"),
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        match self.curr {
            Current::A => self.a.key(),
            Current::B => self.b.key(),
            _ => panic!("TwoMergeIterator: key called on invalid iterator"),
        }
    }

    fn is_valid(&self) -> bool {
        !self.curr.is_none()
    }

    fn next(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.curr, Current::None) {
            Current::A => self.a.next()?,
            Current::B => self.b.next()?,
            _ => panic!("TwoMergeIterator next called on an invalid iterator"),
        }
        if !self.a.is_valid() {
            if self.b.is_valid() {
                self.curr = Current::B;
            }
            return Ok(());
        }

        self.curr = Current::A;
        if self.b.is_valid() {
            let cmp = self.a.key().cmp(&self.b.key());
            match cmp {
                Ordering::Greater => self.curr = Current::B,
                Ordering::Equal => {
                    self.b.next()?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
