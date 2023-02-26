#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use builder::U16_SIZE;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf: Vec<u8> = self.data.clone();
        let offset_len = self.offsets.len();
        for off in &self.offsets {
            buf.put_u16(*off);
        }
        buf.put_u16(offset_len as u16);
        buf.into()
    }

    pub fn decode(datas: &[u8]) -> Self {
        let last_offset = (&datas[datas.len() - U16_SIZE..]).get_u16() as usize; //get_u16表示只读两个字节
        let data_end_off = datas.len() - U16_SIZE - last_offset * U16_SIZE;
        let data = datas[0..data_end_off].to_vec();
        let offset_raw = &datas[data_end_off..datas.len() - U16_SIZE];
        let offsets: Vec<u16> = offset_raw
            .chunks(U16_SIZE)
            .map(|mut x| x.get_u16())
            .collect();
        Self { data, offsets }
    }
}

#[cfg(test)]
mod tests;
