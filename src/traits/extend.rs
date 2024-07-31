use crate::{prelude::*, traits::IdType, traits::Property};

/// Wrapper for errors resulting from [TripleStoreExtend::extend()]
#[derive(Debug)]
pub enum ExtendError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    /// Error from the [TripleStore] being extended.
    Left(LeftError),

    /// Error from the [TripleStore] being consumed.
    Right(RightError),
}

/// A trait for extending a [TripleStore] with elements from another [TripleStore].
///
/// Inserts all nodes and edges from `other` into this [TripleStore], replacing existing property data if present.
pub trait TripleStoreExtend<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreError
{
    /// Extend this [TripleStore] with nodes and edges from `other`.
    ///
    /// Property data for existing nodes will be replaced with data from `other`.
    fn extend<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), ExtendError<Self::Error, E>>;
}
