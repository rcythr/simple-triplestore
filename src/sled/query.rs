use serde::{de::DeserializeOwned, Serialize};

use crate::{
    prelude::*,
    traits::{ConcreteIdType, Property},
    MemTripleStore, Query, QueryError, Triple,
};

use super::SledTripleStore;

impl<
        Id: ConcreteIdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreQuery<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    type QueryResult = MemTripleStore<Id, NodeProps, EdgeProps>;

    fn run(
        &self,
        query: Query<Id>,
    ) -> Result<Self::QueryResult, QueryError<Self::Error, <<Self as TripleStoreQuery<Id, NodeProps, EdgeProps>>::QueryResult as TripleStoreError>::Error>>{
        Ok(match query {
            Query::NodeProps(nodes) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for node in nodes {
                    if let Some(data) = self
                        .node_props
                        .get(&node.to_be_bytes())
                        .map_err(|e| QueryError::Left(super::SledTripleStoreError::SledError(e)))?
                    {
                        result
                            .insert_node(
                                node,
                                bincode::deserialize(&data).map_err(|e| {
                                    QueryError::Left(
                                        super::SledTripleStoreError::SerializationError(e),
                                    )
                                })?,
                            )
                            .map_err(|e| QueryError::Right(e))?;
                    }
                }
                result
            }

            Query::SPO(triples) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (sub, pred, obj) in triples.into_iter() {
                    let triple = Triple { sub, pred, obj };
                    if let Some(data_id) = self
                        .spo_data
                        .get(&Id::encode_spo_triple(&triple))
                        .map_err(|e| QueryError::Left(super::SledTripleStoreError::SledError(e)))?
                    {
                        if let Some(data) = self.edge_props.get(&data_id).map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })? {
                            result
                                .insert_edge(
                                    triple,
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(
                                            super::SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::S(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for sub in items {
                    for r in self.spo_data.range(Id::key_bounds_1(sub)) {
                        let (key, data_id) = r.map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })?;
                        if let Some(data) = self.edge_props.get(&data_id).map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })? {
                            result
                                .insert_edge(
                                    Id::decode_spo_triple(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::SledTripleStoreError::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(
                                            super::SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::SP(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (sub, pred) in items {
                    for r in self.spo_data.range(Id::key_bounds_2(sub, pred)) {
                        let (key, data_id) = r.map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })?;
                        if let Some(data) = self.edge_props.get(&data_id).map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })? {
                            result
                                .insert_edge(
                                    Id::decode_spo_triple(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::SledTripleStoreError::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(
                                            super::SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::SO(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (sub, obj) in items {
                    for r in self.osp_data.range(Id::key_bounds_2(obj, sub)) {
                        let (key, data_id) = r.map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })?;
                        if let Some(data) = self.edge_props.get(&data_id).map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })? {
                            result
                                .insert_edge(
                                    Id::decode_osp_triple(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::SledTripleStoreError::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(
                                            super::SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::P(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for pred in items {
                    for r in self.pos_data.range(Id::key_bounds_1(pred)) {
                        let (key, data_id) = r.map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })?;
                        if let Some(data) = self.edge_props.get(&data_id).map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })? {
                            result
                                .insert_edge(
                                    Id::decode_pos_triple(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::SledTripleStoreError::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(
                                            super::SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::PO(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (pred, obj) in items {
                    for r in self.pos_data.range(Id::key_bounds_2(pred, obj)) {
                        let (key, data_id) = r.map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })?;
                        if let Some(data) = self.edge_props.get(&data_id).map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })? {
                            result
                                .insert_edge(
                                    Id::decode_pos_triple(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::SledTripleStoreError::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(
                                            super::SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::O(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for obj in items {
                    for r in self.osp_data.range(Id::key_bounds_1(obj)) {
                        let (key, data_id) = r.map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })?;
                        if let Some(data) = self.edge_props.get(&data_id).map_err(|e| {
                            QueryError::Left(super::SledTripleStoreError::SledError(e))
                        })? {
                            result
                                .insert_edge(
                                    Id::decode_osp_triple(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::SledTripleStoreError::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(
                                            super::SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{SledTripleStore, UlidIdGenerator};

    #[test]
    fn test_query_node_props() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_node_props(sled_db);
    }

    #[test]
    fn test_query_edge_props() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_edge_props(sled_db);
    }

    #[test]
    fn test_query_s() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_s(sled_db);
    }

    #[test]
    fn test_query_sp() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_sp(sled_db);
    }

    #[test]
    fn test_query_p() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_p(sled_db);
    }

    #[test]
    fn test_query_po() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_po(sled_db);
    }

    #[test]
    fn test_query_o() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_o(sled_db);
    }

    #[test]
    fn test_query_os() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::query::test_query_os(sled_db);
    }
}
