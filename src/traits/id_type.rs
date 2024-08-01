use crate::Triple;

pub trait IdType: std::fmt::Debug + PartialEq + Eq + Clone + std::hash::Hash {}
impl<T: std::fmt::Debug + PartialEq + Eq + Clone + std::hash::Hash> IdType for T {}

pub trait ConcreteIdType:
    IdType
    + std::fmt::Debug
    + std::fmt::Display
    + Clone
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + std::hash::Hash
    + Copy
    + 'static
{
    type ByteArrayType: std::hash::Hash + PartialEq + Ord + AsRef<[u8]> + for<'a> TryFrom<&'a [u8]>;
    type TripleByteArrayType: std::hash::Hash
        + PartialEq
        + Ord
        + AsRef<[u8]>
        + for<'a> TryFrom<&'a [u8]>;

    fn to_be_bytes(self) -> Self::ByteArrayType;
    fn from_be_bytes(bytes: &Self::ByteArrayType) -> Self;
    fn try_from_be_bytes(bytes: &[u8]) -> Option<Self>;

    fn encode_spo_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType;
    fn encode_pos_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType;
    fn encode_osp_triple(triple: &Triple<Self>) -> Self::TripleByteArrayType;

    fn decode_spo_triple(data: &Self::TripleByteArrayType) -> Triple<Self>;
    fn decode_pos_triple(data: &Self::TripleByteArrayType) -> Triple<Self>;
    fn decode_osp_triple(data: &Self::TripleByteArrayType) -> Triple<Self>;

    fn key_bounds_1(
        a: Self,
    ) -> (
        std::ops::Bound<Self::TripleByteArrayType>,
        std::ops::Bound<Self::TripleByteArrayType>,
    );

    fn key_bounds_2(
        a: Self,
        b: Self,
    ) -> (
        std::ops::Bound<Self::TripleByteArrayType>,
        std::ops::Bound<Self::TripleByteArrayType>,
    );
}
