use serde::{de::DeserializeOwned, Serialize};

use crate::{prelude::*, PropertyType};

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreQuery<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    type QueryResultError = ();

    fn run(
        &self,
        query: crate::Query,
    ) -> Result<Self::QueryResult, QueryError<Self::Error, Self::QueryResultError>> {
        Ok(match query {
            Query::NodeProps(nodes) => {
                let mut result = MemTripleStore::new();
                for node in nodes {
                    if let Some(data) = self
                        .node_props
                        .get(&node.0.to_be_bytes())
                        .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                    {
                        result
                            .insert_node(
                                node,
                                bincode::deserialize(&data).map_err(|e| {
                                    QueryError::Left(super::Error::SerializationError(e))
                                })?,
                            )
                            .map_err(|e| QueryError::Right(e))?;
                    }
                }
                result
            }

            Query::SPO(triples) => {
                let mut result = MemTripleStore::new();
                for (sub, pred, obj) in triples.into_iter() {
                    let triple = Triple { sub, pred, obj };
                    if let Some(data_id) = self
                        .spo_data
                        .get(&triple.encode_spo())
                        .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                    {
                        if let Some(data) = self
                            .edge_props
                            .get(&data_id)
                            .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                        {
                            result
                                .insert_edge(
                                    triple,
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(super::Error::SerializationError(e))
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::S(items) => {
                let mut result = MemTripleStore::new();
                for sub in items {
                    for r in self.spo_data.range(Triple::key_bounds_1(sub)) {
                        let (key, data_id) =
                            r.map_err(|e| QueryError::Left(super::Error::SledError(e)))?;
                        if let Some(data) = self
                            .edge_props
                            .get(&data_id)
                            .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                        {
                            result
                                .insert_edge(
                                    Triple::decode_spo(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::Error::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(super::Error::SerializationError(e))
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::SP(items) => {
                let mut result = MemTripleStore::new();
                for (sub, pred) in items {
                    for r in self.spo_data.range(Triple::key_bounds_2(sub, pred)) {
                        let (key, data_id) =
                            r.map_err(|e| QueryError::Left(super::Error::SledError(e)))?;
                        if let Some(data) = self
                            .edge_props
                            .get(&data_id)
                            .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                        {
                            result
                                .insert_edge(
                                    Triple::decode_spo(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::Error::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(super::Error::SerializationError(e))
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::SO(items) => {
                let mut result = MemTripleStore::new();
                for (sub, obj) in items {
                    for r in self.osp_data.range(Triple::key_bounds_2(obj, sub)) {
                        let (key, data_id) =
                            r.map_err(|e| QueryError::Left(super::Error::SledError(e)))?;
                        if let Some(data) = self
                            .edge_props
                            .get(&data_id)
                            .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                        {
                            result
                                .insert_edge(
                                    Triple::decode_osp(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::Error::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(super::Error::SerializationError(e))
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::P(items) => {
                let mut result = MemTripleStore::new();
                for pred in items {
                    for r in self.pos_data.range(Triple::key_bounds_1(pred)) {
                        let (key, data_id) =
                            r.map_err(|e| QueryError::Left(super::Error::SledError(e)))?;
                        if let Some(data) = self
                            .edge_props
                            .get(&data_id)
                            .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                        {
                            result
                                .insert_edge(
                                    Triple::decode_pos(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::Error::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(super::Error::SerializationError(e))
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::PO(items) => {
                let mut result = MemTripleStore::new();
                for (pred, obj) in items {
                    for r in self.pos_data.range(Triple::key_bounds_2(pred, obj)) {
                        let (key, data_id) =
                            r.map_err(|e| QueryError::Left(super::Error::SledError(e)))?;
                        if let Some(data) = self
                            .edge_props
                            .get(&data_id)
                            .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                        {
                            result
                                .insert_edge(
                                    Triple::decode_pos(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::Error::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(super::Error::SerializationError(e))
                                    })?,
                                )
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::O(items) => {
                let mut result = MemTripleStore::new();
                for obj in items {
                    for r in self.osp_data.range(Triple::key_bounds_1(obj)) {
                        let (key, data_id) =
                            r.map_err(|e| QueryError::Left(super::Error::SledError(e)))?;
                        if let Some(data) = self
                            .edge_props
                            .get(&data_id)
                            .map_err(|e| QueryError::Left(super::Error::SledError(e)))?
                        {
                            result
                                .insert_edge(
                                    Triple::decode_osp(&key[..].try_into().map_err(|_| {
                                        QueryError::Left(super::Error::KeySizeError)
                                    })?),
                                    bincode::deserialize(&data).map_err(|e| {
                                        QueryError::Left(super::Error::SerializationError(e))
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
    use crate::prelude::*;

    #[test]
    fn test_query_node_props() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_node_props(sled_db);
    }

    #[test]
    fn test_query_edge_props() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_edge_props(sled_db);
    }

    #[test]
    fn test_query_s() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_s(sled_db);
    }

    #[test]
    fn test_query_sp() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_sp(sled_db);
    }

    #[test]
    fn test_query_p() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_p(sled_db);
    }

    #[test]
    fn test_query_po() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_po(sled_db);
    }

    #[test]
    fn test_query_o() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_o(sled_db);
    }

    #[test]
    fn test_query_os() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::query::test_query_os(sled_db);
    }
}
