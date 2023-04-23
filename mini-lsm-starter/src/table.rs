#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::{self, Block};
use crate::lsm_storage::BlockCache;
use std::os::unix::fs::FileExt;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        let mut estimated_size: usize = 0;
        for block in block_meta {
            estimated_size += std::mem::size_of::<u32>();
            estimated_size += std::mem::size_of::<u16>();
            estimated_size += block.first_key.len();
        }
        buf.reserve(estimated_size);
        let origin_len = buf.len();
        for block in block_meta {
            buf.put_u32(block.offset as u32);
            buf.put_u16(block.first_key.len() as u16);
            buf.put_slice(&block.first_key);
        }
        assert_eq!(estimated_size, buf.len() - origin_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut meta_vec: Vec<BlockMeta> = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32();
            let first_key_len = buf.get_u16();
            let first_key = buf.copy_to_bytes(first_key_len as usize); //copy as bytes
            meta_vec.push(BlockMeta {
                offset: (offset as usize),
                first_key: (first_key),
            });
        }
        meta_vec
    }
}

/// A file object.
// pub struct FileObject(Bytes);

// impl FileObject {
//     pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
//         Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
//     }

//     pub fn size(&self) -> u64 {
//         self.0.len() as u64
//     }

//     /// Create a new file object (day 2) and write the file to the disk (day 4).
//     pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
//         unimplemented!()
//     }

//     pub fn open(path: &Path) -> Result<Self> {
//         unimplemented!()
//     }
// }
pub struct FileObject(File, u64);
impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        self.0.read_exact_at(&mut data[..], offset)?; //read_exact_at function would try to fill the buffer
        Ok(data)
    }
    pub fn size(&self) -> u64 {
        self.1
    }
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        Ok(FileObject(
            File::options().read(true).write(false).open(path)?,
            data.len() as u64,
        ))
    }
}
pub struct SsTable {
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
    block_cache: Option<Arc<BlockCache>>,
    id: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let first_meta_offset = file.read(len - 4, 4)?; //read meta offset
        let block_meta_offset = (&first_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, len - 4 - block_meta_offset)?; //get metafile
        Ok(Self {
            file: (file),
            block_metas: (BlockMeta::decode_block_meta(&raw_meta[..])),
            block_meta_offset: (block_meta_offset as usize),
            block_cache: (block_cache),
            id: (id),
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_metas.get(block_idx).unwrap().offset;
        let offset_end = self
            .block_metas
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_data = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref cache) = self.block_cache {
            let blk = cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`./////////??????
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.block_metas
            .partition_point(|meta| meta.first_key <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
