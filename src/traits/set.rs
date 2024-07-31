use crate::{prelude::*, IdType, Property};

#[derive(Debug)]
pub enum SetOpsError<
    LeftError: std::fmt::Debug,
    RightError: std::fmt::Debug,
    ResultError: std::fmt::Debug,
> {
    Left(LeftError),
    Right(RightError),
    Result(ResultError),
}

/// A trait for basic set operations in a memory-based [TripleStore].
///
/// Provides functionality for union, intersection, and difference operations on sets of triples.
pub trait TripleStoreSetOps<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreError
{
    /// The result type for set operations.
    type SetOpsResult: TripleStore<Id, NodeProps, EdgeProps>;
    type SetOpsResultError: std::fmt::Debug;

    /// Set union of properties and triples with another [TripleStore].
    fn union<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;

    /// Set intersection of properties and triples with another [TripleStore].
    fn intersection<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;

    /// Set difference of properties triples with another [TripleStore].
    fn difference<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;
}
