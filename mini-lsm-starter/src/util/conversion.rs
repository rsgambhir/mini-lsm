use std::collections::HashSet;
use std::hash::Hash;

pub fn to_hashset<T, C>(collection: C) -> HashSet<T>
where
    C: IntoIterator<Item = T>,
    T: Hash + Eq,
{
    collection.into_iter().collect()
}

#[allow(dead_code)]
pub fn to_test_string(b: &[u8]) -> String {
    String::from_utf8(b.to_vec()).unwrap()
}
