use super::{Entity, RdfTripleStore, RdfTripleStoreError};
use crate::{
    traits::{BidirIndex, Property, TripleStore, TripleStoreInsert, TripleStoreInsertBatch},
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
        let id = self.lookup_or_create_entity(&entity)?;
        self.graph
            .insert_node(id, props)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }

    fn insert_edge(&mut self, triple: Triple<Entity>, props: EdgeProps) -> Result<(), Self::Error> {
        let triple = triple.try_map(|entity| self.lookup_or_create_entity(&entity))?;
        self.graph
            .insert_edge(triple, props)
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }
}

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>
            + TripleStoreInsertBatch<Ulid, NodeProps, EdgeProps>,
    > TripleStoreInsertBatch<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    fn insert_batch<T, U>(&mut self, nodes: T, edges: U) -> Result<(), Self::Error>
    where
        T: Iterator<Item = (Entity, NodeProps)>,
        U: Iterator<Item = (Triple<Entity>, EdgeProps)>,
    {
        let nodes = nodes
            .into_iter()
            .map(|(entity, props)| -> Result<_, RdfTripleStoreError<NameIndex::Error, TripleStorage::Error>> {
                Ok((self.lookup_or_create_entity(&entity)?, props))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let edges = edges.into_iter().map(|(triple, props)| -> Result<_, RdfTripleStoreError<NameIndex::Error, TripleStorage::Error>> {
            Ok((triple.try_map(|entity| self.lookup_or_create_entity(&entity))?, props))
        }).collect::<Result<Vec<_>, _>>()?;

        self.graph
            .insert_batch(nodes.into_iter(), edges.into_iter())
            .map_err(|e| RdfTripleStoreError::GraphStorageError(e))
    }
}
