use super::{Entity, RdfTripleStore, RdfTripleStoreError};
use crate::{
    traits::{BidirIndex, Property, TripleStore, TripleStoreInsert},
    Triple,
};
use ulid::Ulid;

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStoreInsert<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    fn insert_node(&mut self, entity: Entity, props: NodeProps) -> Result<(), Self::Error> {
        self.graph
            .insert_node(self.lookup_entity(&entity)?, props)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }

    fn insert_edge(&mut self, triple: Triple<Entity>, props: EdgeProps) -> Result<(), Self::Error> {
        self.graph
            .insert_edge(triple.try_map(|entity| self.lookup_entity(&entity))?, props)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }
}
