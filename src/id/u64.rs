use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::{
    traits::{ConcreteIdType, IdGenerator},
    Triple,
};

impl ConcreteIdType for u64 {
    type ByteArrayType = [u8; 8];
    type TripleByteArrayType = [u8; 24];

    fn to_be_bytes(self) -> Self::ByteArrayType {
        self.to_be_bytes()
    }

    fn from_be_bytes(bytes: &Self::ByteArrayType) -> Self {
        u64::from_be_bytes(*bytes)
    }

    fn try_from_be_bytes(bytes: &[u8]) -> Option<Self> {
        bytes
            .try_into()
            .map(|bytes: &Self::ByteArrayType| Some(u64::from_be_bytes(*bytes)))
            .unwrap_or(None)
    }

    fn encode_spo_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType {
        let mut data = [0u8; 24];
        data[0..8].copy_from_slice(&triple.sub.to_be_bytes());
        data[8..16].copy_from_slice(&triple.pred.to_be_bytes());
        data[16..24].copy_from_slice(&triple.obj.to_be_bytes());
        data
    }

    fn encode_pos_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType {
        let mut data = [0u8; 24];
        data[0..8].copy_from_slice(&triple.pred.to_be_bytes());
        data[8..16].copy_from_slice(&triple.obj.to_be_bytes());
        data[16..24].copy_from_slice(&triple.sub.to_be_bytes());
        data
    }

    fn encode_osp_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType {
        let mut data = [0u8; 24];
        data[0..8].copy_from_slice(&triple.obj.to_be_bytes());
        data[8..16].copy_from_slice(&triple.sub.to_be_bytes());
        data[16..24].copy_from_slice(&triple.pred.to_be_bytes());
        data
    }

    fn decode_spo_triple(data: &Self::TripleByteArrayType) -> Triple<Self> {
        let sub = Self::from_be_bytes(data[0..8].try_into().unwrap());
        let pred = Self::from_be_bytes(data[8..16].try_into().unwrap());
        let obj = Self::from_be_bytes(data[16..24].try_into().unwrap());
        Triple { sub, pred, obj }
    }

    fn decode_pos_triple(data: &Self::TripleByteArrayType) -> Triple<Self> {
        let pred = Self::from_be_bytes(data[0..8].try_into().unwrap());
        let obj = Self::from_be_bytes(data[8..16].try_into().unwrap());
        let sub = Self::from_be_bytes(data[16..24].try_into().unwrap());
        Triple { sub, pred, obj }
    }

    fn decode_osp_triple(data: &Self::TripleByteArrayType) -> Triple<Self> {
        let obj = Self::from_be_bytes(data[0..8].try_into().unwrap());
        let sub = Self::from_be_bytes(data[8..16].try_into().unwrap());
        let pred = Self::from_be_bytes(data[16..24].try_into().unwrap());
        Triple { sub, pred, obj }
    }

    fn key_bounds_1(
        a: Self,
    ) -> (
        std::ops::Bound<Self::TripleByteArrayType>,
        std::ops::Bound<Self::TripleByteArrayType>,
    ) {
        (
            std::ops::Bound::Included(Self::encode_spo_triple(&Triple {
                sub: a,
                pred: u64::MIN,
                obj: u64::MIN,
            })),
            std::ops::Bound::Included(Self::encode_spo_triple(&Triple {
                sub: a,
                pred: u64::MAX,
                obj: u64::MAX,
            })),
        )
    }

    fn key_bounds_2(
        a: Self,
        b: Self,
    ) -> (
        std::ops::Bound<Self::TripleByteArrayType>,
        std::ops::Bound<Self::TripleByteArrayType>,
    ) {
        (
            std::ops::Bound::Included(Self::encode_spo_triple(&Triple {
                sub: a,
                pred: b,
                obj: u64::MIN,
            })),
            std::ops::Bound::Included(Self::encode_spo_triple(&Triple {
                sub: a,
                pred: b,
                obj: u64::MAX,
            })),
        )
    }
}

struct U64IdGenerator {
    state: Arc<AtomicU64>,
}

impl U64IdGenerator {
    pub fn new(initial_value: u64) -> Self {
        Self {
            state: Arc::new(AtomicU64::new(initial_value)),
        }
    }
}

impl IdGenerator<u64> for U64IdGenerator {
    fn clone(&self) -> Box<dyn IdGenerator<u64>> {
        Box::new(Self {
            state: self.state.clone(),
        })
    }

    fn fresh(&mut self) -> u64 {
        self.state.fetch_add(1, Ordering::SeqCst)
    }
}
