use ulid::Ulid;

use super::Triple;

impl Triple {
    pub fn decode_spo(data: &[u8; 48]) -> Self {
        let sub = Ulid(u128::from_be_bytes(data[0..16].try_into().unwrap()));
        let pred = Ulid(u128::from_be_bytes(data[16..32].try_into().unwrap()));
        let obj = Ulid(u128::from_be_bytes(data[32..48].try_into().unwrap()));
        Self { sub, pred, obj }
    }

    pub fn decode_pos(data: &[u8; 48]) -> Self {
        let pred = Ulid(u128::from_be_bytes(data[0..16].try_into().unwrap()));
        let obj = Ulid(u128::from_be_bytes(data[16..32].try_into().unwrap()));
        let sub = Ulid(u128::from_be_bytes(data[32..48].try_into().unwrap()));
        Self { sub, pred, obj }
    }

    pub fn decode_osp(data: &[u8; 48]) -> Self {
        let obj = Ulid(u128::from_be_bytes(data[0..16].try_into().unwrap()));
        let sub = Ulid(u128::from_be_bytes(data[16..32].try_into().unwrap()));
        let pred = Ulid(u128::from_be_bytes(data[32..48].try_into().unwrap()));
        Self { sub, pred, obj }
    }
}
