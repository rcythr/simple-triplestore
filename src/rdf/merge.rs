use super::{Entity, RdfTripleStore, RdfTripleStoreError};
use crate::{
    traits::{BidirIndex, Property, TripleStore, TripleStoreMerge},
    EdgeOrder, MergeError, Mergeable,
};
use ulid::Ulid;

impl<
        NodeProps: Property + Mergeable,
        EdgeProps: Property + Mergeable,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps> + TripleStoreMerge<Ulid, NodeProps, EdgeProps>,
    > TripleStoreMerge<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    fn merge<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<Entity, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), crate::MergeError<Self::Error, E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(EdgeOrder::SPO);

        for r in other_nodes {
            let (id, props) = r.map_err(|e| MergeError::Right(e))?;

            self.merge_node(id, props)
                .map_err(|e| MergeError::Left(e))?;
        }

        for r in other_edges {
            let (triple, props) = r.map_err(|e| MergeError::Right(e))?;

            self.merge_edge(triple, props)
                .map_err(|e| MergeError::Left(e))?;
        }

        Ok(())
    }

    fn merge_node(&mut self, entity: Entity, props: NodeProps) -> Result<(), Self::Error> {
        self.graph
            .merge_node(self.lookup_entity(&entity)?, props)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }

    fn merge_edge(
        &mut self,
        triple: crate::Triple<Entity>,
        props: EdgeProps,
    ) -> Result<(), Self::Error> {
        self.graph
            .merge_edge(triple.try_map(|entity| self.lookup_entity(&entity))?, props)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }
}
