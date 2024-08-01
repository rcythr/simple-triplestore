use std::sync::Arc;

use ulid::Ulid;

use crate::traits::{BidirIndex, Property, TripleStore, TripleStoreError};

mod extend;
mod insert;
mod iter;
mod merge;
mod query;
mod remove;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
enum Entity {
    String(String),
    Ulid(Ulid),
}

#[derive(Debug)]
enum RdfTripleStoreError<NameIndexStorageError, GraphStorageError> {
    NameIndexStorageError(NameIndexStorageError),
    GraphStorageError(GraphStorageError),
    NameNotFound(String),
}

struct RdfTripleStore<
    NodeProps: Property,
    EdgeProps: Property,
    NameIndex: BidirIndex<Left = String, Right = Ulid>,
    TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
> {
    name_index: NameIndex,
    graph: TripleStorage,
    _phantom: std::marker::PhantomData<(NodeProps, EdgeProps)>,
}

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    pub fn new(name_index: NameIndex, graph: TripleStorage) -> Self {
        Self {
            name_index,
            graph,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn lookup_entity(
        &self,
        entity: &Entity,
    ) -> Result<Ulid, RdfTripleStoreError<NameIndex::Error, TripleStorage::Error>> {
        match entity {
            Entity::String(s) => self
                .name_index
                .left_to_right(&s)
                .map_err(|e| RdfTripleStoreError::NameIndexStorageError(e))?
                .ok_or(RdfTripleStoreError::NameNotFound(s.clone())),
            Entity::Ulid(id) => Ok(id.clone()),
        }
    }

    pub fn lookup_id(
        name_index: &NameIndex,
        id: &Ulid,
    ) -> Result<Entity, RdfTripleStoreError<NameIndex::Error, TripleStorage::Error>> {
        Ok(
            match name_index
                .right_to_left(id)
                .map_err(|e| RdfTripleStoreError::NameIndexStorageError(e))?
            {
                Some(s) => Entity::String(s),
                None => Entity::Ulid(id.clone()),
            },
        )
    }
}

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStore<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
}

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStoreError for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    type Error = RdfTripleStoreError<NameIndex::Error, TripleStorage::Error>;
}

#[cfg(test)]
mod test {
    use ulid::Ulid;

    use crate::{MemTripleStore, UlidIdGenerator};

    use crate::mem::MemHashIndex;

    use super::RdfTripleStore;

    #[test]
    fn test_new() {
        let rdf_graph = RdfTripleStore::new(
            MemHashIndex::new(),
            MemTripleStore::<Ulid, (), ()>::new(UlidIdGenerator::new()),
        );
    }
}
