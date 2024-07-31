use crate::{prelude::*, IdType, Property};

/// Wrapper for errors resulting from [TripleStoreMerge::merge()]
#[derive(Debug)]
pub enum MergeError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    /// Error from the [TripleStore] being merged _into_.
    Left(LeftError),

    /// Error from the [TripleStore] being merged _from_.
    Right(RightError),
}

/// A trait for supporting merging in [TripleStore]s.
///
/// If `NodeProps` and `EdgeProps` support the [Mergeable] trait, this trait provides functionality to
/// merge elements from another [TripleStore], merging properties rather than replacing them.
pub trait TripleStoreMerge<
    Id: IdType,
    NodeProps: Property + Mergeable,
    EdgeProps: Property + Mergeable,
>: TripleStoreError
{
    /// Merge all elements from `other` into this [TripleStore].
    ///
    /// Duplicate elements will be merged using the `Mergeable` trait's merge operation.
    fn merge<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>>;

    /// Merge a single node with `id` and `props`.
    fn merge_node(&mut self, node: Id, props: NodeProps) -> Result<(), Self::Error>;

    //// Merge a collection of edges with `(id, props)`.
    fn merge_edge(&mut self, triple: Triple<Id>, props: EdgeProps) -> Result<(), Self::Error>;
}
