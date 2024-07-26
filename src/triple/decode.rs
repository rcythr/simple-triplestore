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

#[cfg(test)]
mod test {
    use ulid::Ulid;

    use crate::Triple;

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
    fn test_decode_spo() {
        assert_eq!(
            Triple::decode_spo(&make_data()),
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
            Triple::decode_pos(&make_data()),
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
            Triple::decode_osp(&make_data()),
            Triple {
                obj: Ulid(0x000102030405060708090A0B0C0D0E0F),
                sub: Ulid(0x101112131415161718191A1B1C1D1E1F),
                pred: Ulid(0x202122232425262728292A2B2C2D2E2F),
            }
        )
    }
}
