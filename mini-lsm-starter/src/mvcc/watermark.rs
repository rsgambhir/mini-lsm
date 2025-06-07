use std::collections::BTreeMap;
use std::ops::AddAssign;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        self.readers.entry(ts).or_insert(0).add_assign(1)
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let cnt = self.readers.get_mut(&ts).unwrap();
        if *cnt == 1 {
            self.readers.remove(&ts);
        } else {
            *cnt -= 1;
        }
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.first_key_value().map(|x| *x.0)
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}
