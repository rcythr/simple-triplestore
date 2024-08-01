use super::{Entity, RdfTripleStore, RdfTripleStoreError};
use crate::traits::{BidirIndex, Property, TripleStore, TripleStoreRemove};
use ulid::Ulid;

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStoreRemove<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    fn remove_node(&mut self, entity: impl std::borrow::Borrow<Entity>) -> Result<(), Self::Error> {
        self.graph
            .remove_node(self.lookup_entity(entity.borrow())?)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }

    fn remove_edge(&mut self, triple: crate::Triple<Entity>) -> Result<(), Self::Error> {
        self.graph
            .remove_edge(triple.try_map(|entity| self.lookup_entity(&entity))?)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }
}
