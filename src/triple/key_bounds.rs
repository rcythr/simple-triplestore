use ulid::Ulid;

use super::Triple;

impl Triple {
    pub fn key_bounds_s(sub: Ulid) -> (std::ops::Bound<[u8; 48]>, std::ops::Bound<[u8; 48]>) {
        (
            std::ops::Bound::Included(
                Triple {
                    sub: sub,
                    pred: Ulid(u128::MIN),
                    obj: Ulid(u128::MIN),
                }
                .encode_spo(),
            ),
            std::ops::Bound::Included(
                Triple {
                    sub: sub,
                    pred: Ulid(u128::MAX),
                    obj: Ulid(u128::MAX),
                }
                .encode_spo(),
            ),
        )
    }

    pub fn key_bounds_sp(
        sub: Ulid,
        pred: Ulid,
    ) -> (std::ops::Bound<[u8; 48]>, std::ops::Bound<[u8; 48]>) {
        (
            std::ops::Bound::Included(
                Triple {
                    sub: sub,
                    pred: pred,
                    obj: Ulid(u128::MIN),
                }
                .encode_spo(),
            ),
            std::ops::Bound::Included(
                Triple {
                    sub: sub,
                    pred: pred,
                    obj: Ulid(u128::MAX),
                }
                .encode_spo(),
            ),
        )
    }

    pub fn key_bounds_p(pred: Ulid) -> (std::ops::Bound<[u8; 48]>, std::ops::Bound<[u8; 48]>) {
        (
            std::ops::Bound::Included(
                Triple {
                    sub: Ulid(u128::MIN),
                    pred: pred,
                    obj: Ulid(u128::MIN),
                }
                .encode_pos(),
            ),
            std::ops::Bound::Included(
                Triple {
                    sub: Ulid(u128::MAX),
                    pred: pred,
                    obj: Ulid(u128::MAX),
                }
                .encode_pos(),
            ),
        )
    }

    pub fn key_bounds_po(
        pred: Ulid,
        obj: Ulid,
    ) -> (std::ops::Bound<[u8; 48]>, std::ops::Bound<[u8; 48]>) {
        (
            std::ops::Bound::Included(
                Triple {
                    sub: Ulid(u128::MIN),
                    pred: pred,
                    obj: obj,
                }
                .encode_pos(),
            ),
            std::ops::Bound::Included(
                Triple {
                    sub: Ulid(u128::MAX),
                    pred: pred,
                    obj: obj,
                }
                .encode_pos(),
            ),
        )
    }

    pub fn key_bounds_o(obj: Ulid) -> (std::ops::Bound<[u8; 48]>, std::ops::Bound<[u8; 48]>) {
        (
            std::ops::Bound::Included(
                Triple {
                    sub: Ulid(u128::MIN),
                    pred: Ulid(u128::MIN),
                    obj: obj,
                }
                .encode_osp(),
            ),
            std::ops::Bound::Included(
                Triple {
                    sub: Ulid(u128::MAX),
                    pred: Ulid(u128::MAX),
                    obj: obj,
                }
                .encode_osp(),
            ),
        )
    }

    pub fn key_bounds_os(
        obj: Ulid,
        sub: Ulid,
    ) -> (std::ops::Bound<[u8; 48]>, std::ops::Bound<[u8; 48]>) {
        (
            std::ops::Bound::Included(
                Triple {
                    sub: sub,
                    pred: Ulid(u128::MIN),
                    obj: obj,
                }
                .encode_osp(),
            ),
            std::ops::Bound::Included(
                Triple {
                    sub: sub,
                    pred: Ulid(u128::MAX),
                    obj: obj,
                }
                .encode_osp(),
            ),
        )
    }
}
