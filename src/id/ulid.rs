use ulid::Ulid;

use crate::{traits::IdGenerator, IdType, Triple};

impl IdType for Ulid {
    type ByteArrayType = [u8; 16];
    type TripleByteArrayType = [u8; 48];

    fn to_be_bytes(self) -> Self::ByteArrayType {
        self.0.to_be_bytes()
    }

    fn from_be_bytes(bytes: &Self::ByteArrayType) -> Self {
        Ulid(u128::from_be_bytes(*bytes))
    }

    fn try_from_be_bytes(bytes: &[u8]) -> Option<Self> {
        bytes
            .try_into()
            .map(|bytes: &Self::ByteArrayType| Some(Ulid(u128::from_be_bytes(*bytes))))
            .unwrap_or(None)
    }

    fn encode_spo_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType {
        let mut data = [0u8; 48];
        data[0..16].copy_from_slice(&triple.sub.0.to_be_bytes());
        data[16..32].copy_from_slice(&triple.pred.0.to_be_bytes());
        data[32..48].copy_from_slice(&triple.obj.0.to_be_bytes());
        data
    }

    fn encode_pos_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType {
        let mut data = [0u8; 48];
        data[0..16].copy_from_slice(&triple.pred.0.to_be_bytes());
        data[16..32].copy_from_slice(&triple.obj.0.to_be_bytes());
        data[32..48].copy_from_slice(&triple.sub.0.to_be_bytes());
        data
    }

    fn encode_osp_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType {
        let mut data = [0u8; 48];
        data[0..16].copy_from_slice(&triple.obj.0.to_be_bytes());
        data[16..32].copy_from_slice(&triple.sub.0.to_be_bytes());
        data[32..48].copy_from_slice(&triple.pred.0.to_be_bytes());
        data
    }

    fn decode_spo_triple(data: &Self::TripleByteArrayType) -> Triple<Self> {
        let sub = Ulid(u128::from_be_bytes(data[0..16].try_into().unwrap()));
        let pred = Ulid(u128::from_be_bytes(data[16..32].try_into().unwrap()));
        let obj = Ulid(u128::from_be_bytes(data[32..48].try_into().unwrap()));
        Triple { sub, pred, obj }
    }

    fn decode_pos_triple(data: &Self::TripleByteArrayType) -> Triple<Self> {
        let pred = Ulid(u128::from_be_bytes(data[0..16].try_into().unwrap()));
        let obj = Ulid(u128::from_be_bytes(data[16..32].try_into().unwrap()));
        let sub = Ulid(u128::from_be_bytes(data[32..48].try_into().unwrap()));
        Triple { sub, pred, obj }
    }

    fn decode_osp_triple(data: &Self::TripleByteArrayType) -> Triple<Self> {
        let obj = Ulid(u128::from_be_bytes(data[0..16].try_into().unwrap()));
        let sub = Ulid(u128::from_be_bytes(data[16..32].try_into().unwrap()));
        let pred = Ulid(u128::from_be_bytes(data[32..48].try_into().unwrap()));
        Triple { sub, pred, obj }
    }

    fn key_bounds_1(a: Self) -> (std::ops::Bound<[u8; 48]>, std::ops::Bound<[u8; 48]>) {
        (
            std::ops::Bound::Included(Ulid::encode_spo_triple(&Triple {
                sub: a,
                pred: Ulid(u128::MIN),
                obj: Ulid(u128::MIN),
            })),
            std::ops::Bound::Included(Ulid::encode_spo_triple(&Triple {
                sub: a,
                pred: Ulid(u128::MAX),
                obj: Ulid(u128::MAX),
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
                obj: Ulid(u128::MIN),
            })),
            std::ops::Bound::Included(Self::encode_spo_triple(&Triple {
                sub: a,
                pred: b,
                obj: Ulid(u128::MAX),
            })),
        )
    }
}

#[derive(Clone)]
pub struct UlidIdGenerator {}

impl UlidIdGenerator {
    pub fn new() -> Self {
        Self {}
    }
}

impl IdGenerator<Ulid> for UlidIdGenerator {
    fn clone(&self) -> Box<dyn IdGenerator<Ulid>> {
        Box::new(UlidIdGenerator::new())
    }

    fn fresh(&mut self) -> Ulid {
        Ulid::new()
    }
}

#[cfg(test)]
mod test {
    use crate::{traits::IdType, Triple};
    use std::ops::Bound::Included;
    use ulid::Ulid;

    fn make_triple() -> Triple<Ulid> {
        Triple {
            sub: Ulid(0x000102030405060708090A0B0C0D0E0F),
            pred: Ulid(0x101112131415161718191A1B1C1D1E1F),
            obj: Ulid(0x202122232425262728292A2B2C2D2E2F),
        }
    }

    fn make_data() -> [u8; 48] {
        [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
            0x0E, 0x0F, //
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
            0x1E, 0x1F, //
            0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D,
            0x2E, 0x2F,
        ]
    }

    #[test]
    fn test_encode_spo() {
        assert_eq!(
            Ulid::encode_spo_triple(&make_triple()),
            [
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                0x0E, 0x0F, //
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                0x1E, 0x1F, //
                0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D,
                0x2E, 0x2F
            ]
        )
    }

    #[test]
    fn test_decode_spo() {
        assert_eq!(
            Ulid::decode_spo_triple(&make_data()),
            Triple {
                sub: Ulid(0x000102030405060708090A0B0C0D0E0F),
                pred: Ulid(0x101112131415161718191A1B1C1D1E1F),
                obj: Ulid(0x202122232425262728292A2B2C2D2E2F),
            }
        )
    }

    #[test]
    fn test_encode_pos() {
        assert_eq!(
            Ulid::encode_pos_triple(&make_triple()),
            [
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                0x1E, 0x1F, //
                0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D,
                0x2E, 0x2F, //
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                0x0E, 0x0F,
            ]
        )
    }

    #[test]
    fn test_decode_pos() {
        assert_eq!(
            Ulid::decode_pos_triple(&make_data()),
            Triple {
                pred: Ulid(0x000102030405060708090A0B0C0D0E0F),
                obj: Ulid(0x101112131415161718191A1B1C1D1E1F),
                sub: Ulid(0x202122232425262728292A2B2C2D2E2F),
            }
        )
    }

    #[test]
    fn test_encode_osp() {
        assert_eq!(
            Ulid::encode_osp_triple(&make_triple()),
            [
                0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2A, 0x2B, 0x2C, 0x2D,
                0x2E, 0x2F, //
                0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
                0x0E, 0x0F, //
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D,
                0x1E, 0x1F,
            ]
        )
    }

    #[test]
    fn test_decode_osp() {
        assert_eq!(
            Ulid::decode_osp_triple(&make_data()),
            Triple {
                obj: Ulid(0x000102030405060708090A0B0C0D0E0F),
                sub: Ulid(0x101112131415161718191A1B1C1D1E1F),
                pred: Ulid(0x202122232425262728292A2B2C2D2E2F),
            }
        )
    }

    fn id_1() -> Ulid {
        Ulid(0xDEADBEEFDEADBEEFDEADBEEFDEADBEEF)
    }

    fn id_2() -> Ulid {
        Ulid(0xCAFEBABECAFEBABECAFEBABECAFEBABE)
    }

    #[test]
    fn test_key_bounds_1() {
        if let (Included(lb), Included(ub)) = Ulid::key_bounds_1(id_1()) {
            assert_eq!(
                lb,
                [
                    0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE,
                    0xAD, 0xBE, 0xEF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                ]
            );
            assert_eq!(
                ub,
                [
                    0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE,
                    0xAD, 0xBE, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
                ]
            );
        } else {
            //Bounds should be included on both ends.
            assert!(false);
        }
    }

    #[test]
    fn test_key_bounds_2() {
        if let (Included(lb), Included(ub)) = Ulid::key_bounds_2(id_1(), id_2()) {
            assert_eq!(
                lb,
                [
                    0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE,
                    0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0xCA, 0xFE, 0xBA, 0xBE, 0xCA, 0xFE,
                    0xBA, 0xBE, 0xCA, 0xFE, 0xBA, 0xBE, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                ]
            );
            assert_eq!(
                ub,
                [
                    0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE, 0xAD, 0xBE, 0xEF, 0xDE,
                    0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE, 0xCA, 0xFE, 0xBA, 0xBE, 0xCA, 0xFE,
                    0xBA, 0xBE, 0xCA, 0xFE, 0xBA, 0xBE, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
                    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF
                ]
            );
        } else {
            //Bounds should be included on both ends.
            assert!(false);
        }
    }
}
