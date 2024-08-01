use crate::traits::IndexType;

/// Bidirectional Index
pub trait BidirIndex {
    type Left: IndexType;
    type Right: IndexType;
    type Error: std::fmt::Debug;

    fn set(&mut self, key: Self::Left, id: Self::Right) -> Result<(), Self::Error>;

    fn left_to_right(&self, key: &Self::Left) -> Result<Option<Self::Right>, Self::Error>;

    fn right_to_left(&self, id: &Self::Right) -> Result<Option<Self::Left>, Self::Error>;
}
