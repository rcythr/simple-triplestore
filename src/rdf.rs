use ulid::Ulid;

use crate::traits::{BidirIndex, Property, TripleStore, TripleStoreError};

mod extend;
mod insert;
mod iter;
mod merge;
mod query;
mod remove;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum Entity {
    String(String),
    Ulid(Ulid),
}

impl From<String> for Entity {
    fn from(value: String) -> Self {
        Entity::String(value)
    }
}

impl From<&str> for Entity {
    fn from(value: &str) -> Self {
        Entity::String(value.to_string())
    }
}

impl From<Ulid> for Entity {
    fn from(value: Ulid) -> Self {
        Entity::Ulid(value)
    }
}

#[derive(Debug)]
pub enum RdfTripleStoreError<NameIndexStorageError, GraphStorageError> {
    NameIndexStorageError(NameIndexStorageError),
    GraphStorageError(GraphStorageError),
    #[allow(dead_code)]
    NameNotFound(String),
}

pub struct RdfTripleStore<
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

    pub fn get_name_index(&self) -> &NameIndex {
        &self.name_index
    }

    pub fn get_graph(&self) -> &TripleStorage {
        &self.graph
    }

    pub fn get_name_index_mut(&mut self) -> &mut NameIndex {
        &mut self.name_index
    }

    pub fn get_graph_mut(&mut self) -> &mut TripleStorage {
        &mut self.graph
    }

    pub fn lookup_or_create_entity(
        &mut self,
        entity: &Entity,
    ) -> Result<Ulid, RdfTripleStoreError<NameIndex::Error, TripleStorage::Error>> {
        match entity {
            Entity::String(s) => {
                let result = self
                    .name_index
                    .left_to_right(&s)
                    .map_err(|e| RdfTripleStoreError::NameIndexStorageError(e))?;

                if let Some(id) = result {
                    Ok(id)
                } else {
                    let id = Ulid::new();
                    self.name_index
                        .set(s.clone(), id)
                        .map_err(|e| RdfTripleStoreError::NameIndexStorageError(e))?;
                    Ok(id)
                }
            }
            Entity::Ulid(id) => Ok(id.clone()),
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
    use std::collections::HashSet;

    use ulid::Ulid;

    use crate::traits::{TripleStoreInsert, TripleStoreIter};
    use crate::{MemTripleStore, PropsTriple, Triple, UlidIdGenerator};

    use crate::mem::MemHashIndex;

    use super::RdfTripleStore;

    #[test]
    fn test_new() {
        let mut rdf_graph = RdfTripleStore::new(
            MemHashIndex::new(),
            MemTripleStore::new(UlidIdGenerator::new()),
        );

        let node_3 = Ulid::new();
        rdf_graph.insert_node("foo".into(), 1).unwrap();
        rdf_graph.insert_node("bar".into(), 2).unwrap();
        rdf_graph.insert_node(node_3.into(), 3).unwrap();

        rdf_graph
            .insert_edge(
                Triple {
                    sub: "foo".into(),
                    pred: "knows".into(),
                    obj: "bar".into(),
                },
                123,
            )
            .unwrap();

        rdf_graph
            .insert_edge(
                Triple {
                    sub: "bar".into(),
                    pred: "knows".into(),
                    obj: node_3.into(),
                },
                456,
            )
            .unwrap();

        assert_eq!(
            rdf_graph
                .iter_edges_with_props(crate::EdgeOrder::SPO)
                .collect::<Result<HashSet<_>, _>>()
                .unwrap(),
            [
                PropsTriple {
                    sub: ("foo".into(), 1),
                    pred: ("knows".into(), 123),
                    obj: ("bar".into(), 2)
                },
                PropsTriple {
                    sub: ("bar".into(), 2),
                    pred: ("knows".into(), 456),
                    obj: (node_3.into(), 3)
                }
            ]
            .into()
        )
    }
}
