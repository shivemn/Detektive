//! A growable set of `u32` ids stored as bits: one bit per possible id.
//!
//! Five million transaction ids take about 0.6 MB this way, against tens of
//! megabytes for a `HashSet<u32>`, and a lookup is one shift and one mask.

#[derive(Debug, Default)]
pub struct BitSet {
    words: Vec<u64>,
    len: usize,
}

impl BitSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add `id`. Returns `true` if it was not there before.
    pub fn insert(&mut self, id: u32) -> bool {
        let (word, bit) = ((id / 64) as usize, id % 64);
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        let mask = 1u64 << bit;
        let fresh = self.words[word] & mask == 0;
        if fresh {
            self.words[word] |= mask;
            self.len += 1;
        }
        fresh
    }

    pub fn contains(&self, id: u32) -> bool {
        self.words
            .get((id / 64) as usize)
            .is_some_and(|w| w & (1u64 << (id % 64)) != 0)
    }

    /// Number of ids in the set.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_reports_new_ids_once() {
        let mut set = BitSet::new();
        assert!(set.insert(5));
        assert!(!set.insert(5));
        assert!(set.insert(64));
        assert!(set.insert(5_000_000));
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn contains_handles_ids_beyond_the_end() {
        let mut set = BitSet::new();
        set.insert(1);
        assert!(set.contains(1));
        assert!(!set.contains(2));
        assert!(!set.contains(u32::MAX));
    }

    #[test]
    fn works_at_word_boundaries() {
        let mut set = BitSet::new();
        for id in [0, 63, 64, 127, 128] {
            assert!(set.insert(id));
        }
        assert!((0..=128).all(|id| set.contains(id) == [0, 63, 64, 127, 128].contains(&id)));
    }
}
