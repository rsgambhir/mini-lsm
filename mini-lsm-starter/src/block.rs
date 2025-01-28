mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        if self.offsets.is_empty() {
            panic!("encoding empty block...")
        }
        let block_size = /* KV data */ self.data.len() +
            /* offsets */ 2 * (self.offsets.len()) + /* num elements */2;

        let mut block_bytes = Vec::new();

        block_bytes.reserve_exact(block_size);
        block_bytes.extend_from_slice(&self.data);

        self.offsets
            .iter()
            .for_each(|o| block_bytes.extend_from_slice(&o.to_le_bytes()));

        let num_elements = self.offsets.len() as u16;
        block_bytes.extend_from_slice(&num_elements.to_le_bytes());

        Bytes::from(block_bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        if data.len() < 2 {
            panic!("block decode: insufficient footer length")
        }
        let num_elements = (&data[data.len() - 2..]).get_u16_le() as usize;
        if data.len() < 2 + 2 * num_elements {
            panic!(
                "block decode: insufficient input length, got: {}, required: {}",
                data.len(),
                2 + 2 * num_elements
            )
        }
        let kv_data_sz = data.len() - 2 - 2 * num_elements;

        let offsets: Vec<u16> = data[kv_data_sz..data.len() - 2]
            .chunks(2)
            .map(|mut o| o.get_u16_le())
            .collect();

        if offsets.is_empty() {
            panic!("decoded empty block...")
        }

        let data = data[0..kv_data_sz].to_vec();
        Self { data, offsets }
    }
}
