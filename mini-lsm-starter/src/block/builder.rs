#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;
use std::mem;

use super::Block;
pub const U16_SIZE: usize = std::mem::size_of::<u16>();
/// Builds a block.
pub struct BlockBuilder {
    offset: Vec<u16>,
    data: Vec<u8>,
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offset: Vec::new(),
            data: Vec::new(),
            block_size,
        }
    }
    pub fn now_size(&self) -> usize {
        self.data.len() * U16_SIZE + self.offset.len() * U16_SIZE + U16_SIZE
    }
    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key should not be empty");
        let now_size = self.now_size() + 3 * U16_SIZE + key.len() + value.len();
        if self.now_size() + 3 * U16_SIZE + key.len() + value.len() > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        self.offset.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offset.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if !self.is_empty() {
            Block {
                data: self.data,
                offsets: self.offset,
            }
        } else {
            panic!("block should not be empty!\n");
        }
    }
}
