#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;
use bytes::Buf;
use std::sync::Arc;
pub const SIZ: usize = 2;
/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }
    //based on id
    fn seek_to(&mut self, id: usize) {
        if id >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        let off = self.block.offsets[id] as usize;
        self.idx = id;
        self.seek_to_offset(off)
    }
    //based on offset
    fn seek_to_offset(&mut self, off: usize) {
        let mut entry = &self.block.data[off..];
        let key_len = entry.get_u16() as usize; //get 之后会自动前进2bytes
        let key = entry[..key_len].to_vec();
        entry.advance(key_len);
        self.key.clear();
        self.key.extend(key);

        let value_len = entry.get_u16() as usize;
        let value = entry[..value_len].to_vec();
        entry.advance(value_len);
        self.value.clear();
        self.value.extend(value);
    }
    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut l: i32 = 0;
        let mut r: i32 = self.block.offsets.len().try_into().unwrap();
        r -= 1;
        let mut m;
        while l <= r {
            m = (l + r) >> 1;
            self.seek_to(m.try_into().unwrap());
            if self.key() < key {
                l = m + 1;
            } else {
                r = m - 1;
            }
        }
        self.seek_to(l.try_into().unwrap());
    }
}
