use std::collections::HashSet;
use std::hash::Hash;

pub fn to_hashset<T, C>(collection: C) -> HashSet<T>
where
    C: IntoIterator<Item = T>,
    T: Hash + Eq,
{
    collection.into_iter().collect()
}
