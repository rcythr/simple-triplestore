use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;

use crate::traits::IdType;
use crate::EdgeOrder;
use crate::Property;
use crate::PropsTriple;
use crate::Triple;
use crate::TripleStoreIntoIter;
use crate::TripleStoreIter;

use super::SledTripleStore;
use super::SledTripleStoreError;

fn decode_id<Id: IdType>(id: IVec) -> Result<Id, SledTripleStoreError> {
    Id::try_from_be_bytes(id.as_ref()).ok_or(SledTripleStoreError::KeySizeError)
}

impl<
        Id: IdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn get_node_data_internal(
        &self,
        id: &Id::ByteArrayType,
    ) -> Result<Option<NodeProps>, SledTripleStoreError> {
        self.node_props
            .get(id)
            .map_err(|e| SledTripleStoreError::SledError(e))?
            .map(|data| {
                bincode::deserialize(&data).map_err(|e| SledTripleStoreError::SerializationError(e))
            })
            .transpose()
    }

    fn get_node_data_by_id(&self, id: &Id) -> Result<Option<NodeProps>, SledTripleStoreError> {
        self.get_node_data_internal(&id.to_be_bytes())
    }

    fn get_edge_data_internal(
        &self,
        id: &sled::IVec,
    ) -> Result<Option<EdgeProps>, SledTripleStoreError> {
        self.edge_props
            .get(id)
            .map_err(|e| SledTripleStoreError::SledError(e))?
            .map(|data| {
                bincode::deserialize(&data).map_err(|e| SledTripleStoreError::SerializationError(e))
            })
            .transpose()
    }

    fn iter_impl(
        &self,
        triple: Triple<Id>,
        v: IVec,
    ) -> Result<PropsTriple<Id, NodeProps, EdgeProps>, SledTripleStoreError> {
        match (
            self.get_node_data_by_id(&triple.sub)?,
            self.get_edge_data_internal(&v)?,
            self.get_node_data_by_id(&triple.obj)?,
        ) {
            (Some(sub_props), Some(prod_props), Some(obj_props)) => Ok(PropsTriple {
                sub: (triple.sub, sub_props),
                pred: (triple.pred, prod_props),
                obj: (triple.obj, obj_props),
            }),
            _ => Err(SledTripleStoreError::MissingPropertyData),
        }
    }
}
impl<
        Id: IdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreIter<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn vertices(&self) -> Result<impl Iterator<Item = Id>, Self::Error> {
        self.node_props
            .iter()
            .map(|r| match r {
                Ok((k, _)) => {
                    let k = decode_id(k)?;
                    Ok(k)
                }
                Err(e) => Err(SledTripleStoreError::SledError(e)),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(|v| v.into_iter())
    }

    fn iter_nodes(
        &self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Id, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>>,
    ) {
        (self.iter_vertices(), self.iter_edges(order))
    }

    fn iter_vertices<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Id, NodeProps), SledTripleStoreError>> {
        self.node_props.iter().map(|r| match r {
            Ok((k, v)) => {
                let k = decode_id(k)?;
                let v = bincode::deserialize(&v)
                    .map_err(|e| SledTripleStoreError::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(SledTripleStoreError::SledError(e)),
        })
    }

    fn iter_edges_with_props<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Id, NodeProps, EdgeProps>, SledTripleStoreError>> + 'a
    {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_spo_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_pos_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_osp_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
        };
        edges.map(|r| r.and_then(|(k, v)| self.iter_impl(k, v)))
    }

    fn iter_edges<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Id>, EdgeProps), SledTripleStoreError>> + 'a {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_spo_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_pos_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_osp_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
        };

        edges.map(|r| {
            r.and_then(|(k, v)| {
                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((k, pred_data))
                } else {
                    Err(SledTripleStoreError::MissingPropertyData)
                }
            })
        })
    }
}

impl<
        Id: IdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreIntoIter<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn into_iter_nodes(
        self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Id, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>>,
    ) {
        let node_iter = self.node_props.into_iter().map(|r| match r {
            Ok((k, v)) => {
                let k = decode_id(k)?;
                let v = bincode::deserialize(&v)
                    .map_err(|e| SledTripleStoreError::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(SledTripleStoreError::SledError(e)),
        });

        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_spo_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_pos_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_osp_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
        };

        let edge_iter = edges.map(move |r| {
            r.and_then(|(k, v)| {
                let pred_data = self
                    .edge_props
                    .get(v)
                    .map_err(|e| SledTripleStoreError::SledError(e))?
                    .map(|data| {
                        bincode::deserialize(&data)
                            .map_err(|e| SledTripleStoreError::SerializationError(e))
                    })
                    .transpose();

                if let Some(pred_data) = pred_data? {
                    Ok((k, pred_data))
                } else {
                    Err(SledTripleStoreError::MissingPropertyData)
                }
            })
        });
        (node_iter, edge_iter)
    }

    fn into_iter_vertices(self) -> impl Iterator<Item = Result<(Id, NodeProps), Self::Error>> {
        self.node_props.into_iter().map(|r| match r {
            Ok((k, v)) => {
                let k = decode_id(k)?;
                let v = bincode::deserialize(&v)
                    .map_err(|e| SledTripleStoreError::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(SledTripleStoreError::SledError(e)),
        })
    }

    fn into_iter_edges_with_props(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Id, NodeProps, EdgeProps>, Self::Error>> {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.into_iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_spo_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.into_iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_pos_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.into_iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_osp_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
        };
        edges.map(move |r| r.and_then(|(k, v)| self.iter_impl(k, v)))
    }

    fn into_iter_edges(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>> {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_spo_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_pos_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| SledTripleStoreError::SledError(e))
                    .and_then(|(k, v)| {
                        Ok((
                            Id::decode_osp_triple(
                                &k[..]
                                    .try_into()
                                    .map_err(|_| SledTripleStoreError::KeySizeError)?,
                            ),
                            v,
                        ))
                    })
            })),
        };

        edges.map(move |r| {
            r.and_then(|(k, v)| {
                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((k, pred_data))
                } else {
                    Err(SledTripleStoreError::MissingPropertyData)
                }
            })
        })
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_iter_spo() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_iter_spo(sled_db);
    }

    #[test]
    fn test_iter_pos() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_iter_pos(sled_db);
    }

    #[test]
    fn test_iter_osp() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_iter_osp(sled_db);
    }

    #[test]
    fn test_iter_edge_spo() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_iter_edge_spo(sled_db);
    }

    #[test]
    fn test_iter_edge_pos() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_iter_edge_pos(sled_db);
    }

    #[test]
    fn test_iter_edge_osp() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_iter_edge_osp(sled_db);
    }

    #[test]
    fn test_iter_node() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_iter_node(sled_db);
    }

    #[test]
    fn test_into_iter_spo() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_into_iter_spo(sled_db);
    }

    #[test]
    fn test_into_iter_pos() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_into_iter_pos(sled_db);
    }

    #[test]
    fn test_into_iter_osp() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_into_iter_osp(sled_db);
    }

    #[test]
    fn test_into_iter_edge_spo() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_into_iter_edge_spo(sled_db);
    }

    #[test]
    fn test_into_iter_edge_pos() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_into_iter_edge_pos(sled_db);
    }

    #[test]
    fn test_into_iter_edge_osp() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_into_iter_edge_osp(sled_db);
    }

    #[test]
    fn test_into_iter_node() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::iter::test_into_iter_node(sled_db);
    }
}
