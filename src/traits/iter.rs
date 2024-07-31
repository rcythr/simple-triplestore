use crate::{prelude::*, IdType, Property};

// Iteration functions which do not consume the TripleStore.
pub trait TripleStoreIter<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreError
{
    // Return the identifiers for all verticies. The result is lifted out of the iterator for easy usage by the caller.
    fn vertices(&self) -> Result<impl Iterator<Item = Id>, Self::Error>;

    // Return two iterators: one for vertices, and one for edges.
    fn iter_nodes(
        &self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Id, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>>,
    );

    /// Iterate over vertices in the triplestore.
    fn iter_vertices<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Id, NodeProps), Self::Error>> + 'a;

    /// Iterate over the edges in the triplestore, fetching node properties for each subject and object.
    fn iter_edges_with_props<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Id, NodeProps, EdgeProps>, Self::Error>> + 'a;

    /// Iterate over the edges in the triplestore
    fn iter_edges<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>> + 'a;
}
