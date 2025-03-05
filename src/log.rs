use serde::{Deserialize, Serialize};

use super::message::Message;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub term: usize,
    pub op: Option<Message>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Log {
    entries: Vec<Entry>,
}

impl Log {
    pub fn new() -> Self {
        Self { entries: vec![] }
    }

    pub fn get(&self, index: usize) -> Option<&Entry> {
        self.entries.get(index - 1)
    }

    pub fn append(&mut self, entries: &mut Vec<Entry>) {
        self.entries.append(entries);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn last_term(&self) -> usize {
        self.entries
            .last()
            .expect("append-only Log should never be empty")
            .term
    }

    pub fn from_index_till_end(&self, index: usize) -> &[Entry] {
        &self.entries[(index - 1)..]
    }

    pub fn discard_after(&mut self, index: usize) {
        self.entries.truncate(index);
    }
}
