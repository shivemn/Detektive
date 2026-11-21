//! Turns "bank:account" strings into dense `u32` ids.
//!
//! An account number alone is not unique in the IBM data; it is only unique
//! together with its bank, so the key is both. Ids are handed out in order of
//! first appearance, so they are stable for a given input file.

use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct AccountIndex {
    ids: HashMap<String, u32>,
    keys: Vec<String>,
    // reused for lookups so a known account costs no allocation
    scratch: String,
}

impl AccountIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Id for this bank and account, creating one if it is new.
    pub fn intern(&mut self, bank: &str, account: &str) -> u32 {
        self.scratch.clear();
        self.scratch.push_str(bank);
        self.scratch.push(':');
        self.scratch.push_str(account);

        if let Some(&id) = self.ids.get(self.scratch.as_str()) {
            return id;
        }
        let id = u32::try_from(self.keys.len()).expect("more than 4 billion accounts");
        self.keys.push(self.scratch.clone());
        self.ids.insert(self.scratch.clone(), id);
        id
    }

    /// The original "bank:account" key for an id.
    pub fn key(&self, id: u32) -> Option<&str> {
        self.keys.get(id as usize).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_account_gets_same_id() {
        let mut index = AccountIndex::new();
        let a = index.intern("010", "8000EBD30");
        let b = index.intern("3208", "8000F4580");
        assert_eq!(index.intern("010", "8000EBD30"), a);
        assert_ne!(a, b);
        assert_eq!(index.len(), 2);
    }

    #[test]
    fn same_account_number_at_different_banks_is_different() {
        let mut index = AccountIndex::new();
        assert_ne!(index.intern("001", "ABC"), index.intern("002", "ABC"));
    }

    #[test]
    fn ids_map_back_to_keys() {
        let mut index = AccountIndex::new();
        let id = index.intern("03208", "8000F4580");
        assert_eq!(index.key(id), Some("03208:8000F4580"));
        assert_eq!(index.key(99), None);
    }
}
