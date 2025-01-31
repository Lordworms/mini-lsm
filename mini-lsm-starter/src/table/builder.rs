#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{self, BlockBuilder},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    block_builder: BlockBuilder,
    data: Vec<u8>,
    first_key: Vec<u8>,
    pub(super) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_builder: (BlockBuilder::new(block_size)),
            data: (Vec::new()),
            first_key: (Vec::new()),
            meta: (Vec::new()),
            block_size: (block_size),
        }
    }
    fn add_block(&mut self) {
        //first replace the original builder with new builder
        let old_block_builder =
            std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        //encode original block
        let old_encode_block = old_block_builder.build().encode();
        //push the offset to meta
        self.meta.push(BlockMeta {
            offset: (self.data.len()),
            first_key: (std::mem::take(&mut self.first_key).into()),
        });
        //extend the original data
        self.data.extend(old_encode_block); //data contains blocks
    }
    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }
        if self.block_builder.add(key, value) {
            return;
        }
        self.add_block();
        assert!(self.block_builder.add(key, value));
        self.first_key = key.to_vec();
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.add_block();
        let mut buf: Vec<u8> = self.data;
        let meta_off = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_off as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file: (file),
            block_metas: (self.meta),
            block_meta_offset: (meta_off),
            block_cache,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
