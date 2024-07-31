use crate::{
    prelude::*,
    traits::{IdType, Property},
    Triple,
};

/// A trait for insertion operations in [TripleStore]s.
///
/// Allows insertion of vertices (nodes) and edges, both singularly and in batches.
pub trait TripleStoreInsert<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreError
{
    /// Insert a node with `id` and `props`.
    fn insert_node(&mut self, id: Id, props: NodeProps) -> Result<(), Self::Error>;

    /// Insert an edge with `triple` and `props`.
    ///
    /// <div class="warning">Nodes need not be inserted before edges; however, Orphaned edges (edges referring to missing nodes) are ignored
    /// by iteration functions and higher-order operations.</div>
    fn insert_edge(&mut self, triple: Triple<Id>, props: EdgeProps) -> Result<(), Self::Error>;
}
