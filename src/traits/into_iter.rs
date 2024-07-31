use crate::{
    prelude::*,
    traits::{IdType, Property},
    EdgeOrder, PropsTriple, Triple,
};

pub trait TripleStoreIntoIter<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreError
{
    // Return two iterators: one for vertices, and one for edges.
    fn into_iter_nodes(
        self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Id, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>>,
    );

    /// Iterate over vertices in the triplestore.
    fn into_iter_vertices(self) -> impl Iterator<Item = Result<(Id, NodeProps), Self::Error>>;

    /// Iterate over the edges in the triplestore, fetching node properties for each subject and object.
    fn into_iter_edges_with_props(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Id, NodeProps, EdgeProps>, Self::Error>>;

    /// Iterate over the edges in the triplestore
    fn into_iter_edges(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>>;
}
