use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;
use ulid::Ulid;

use crate::EdgeOrder;
use crate::PropertiesType;
use crate::PropsTriple;
use crate::Triple;
use crate::TripleStoreIntoIter;
use crate::TripleStoreIter;

use super::Error;
use super::SledTripleStore;
impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned,
    > SledTripleStore<NodeProperties, EdgeProperties>
{
    fn get_node_data_internal(&self, id: &Vec<u8>) -> Result<Option<NodeProperties>, Error> {
        self.node_props
            .get(id)
            .map_err(|e| Error::SledError(e))?
            .map(|data| bincode::deserialize(&data).map_err(|e| Error::SerializationError(e)))
            .transpose()
    }

    fn get_node_data_by_id(&self, id: &u128) -> Result<Option<NodeProperties>, Error> {
        self.get_node_data_internal(
            &bincode::serialize(id).map_err(|e| Error::SerializationError(e))?,
        )
    }

    fn get_edge_data_internal(&self, id: &sled::IVec) -> Result<Option<EdgeProperties>, Error> {
        self.edge_props
            .get(id)
            .map_err(|e| Error::SledError(e))?
            .map(|data| bincode::deserialize(&data).map_err(|e| Error::SerializationError(e)))
            .transpose()
    }

    fn iter_impl(
        &self,
        triple: Triple,
        v: IVec,
    ) -> Result<PropsTriple<NodeProperties, EdgeProperties>, Error> {
        match (
            self.get_node_data_by_id(&triple.sub.0)?,
            self.get_edge_data_internal(&v)?,
            self.get_node_data_by_id(&triple.obj.0)?,
        ) {
            (Some(sub_props), Some(prod_props), Some(obj_props)) => Ok(PropsTriple {
                sub: (triple.sub, sub_props),
                pred: (triple.pred, prod_props),
                obj: (triple.obj, obj_props),
            }),
            _ => Err(Error::MissingPropertyData),
        }
    }
}
impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned,
    > TripleStoreIter<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn iter_nodes(
        &self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>,
        impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>,
    ) {
        (self.iter_vertices(), self.iter_edges(order))
    }

    fn iter_vertices<'a>(&'a self) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Error>> {
        self.node_props.iter().map(|r| match r {
            Ok((k, v)) => {
                let k = Ulid(bincode::deserialize(&k).map_err(|e| Error::SerializationError(e))?);
                let v = bincode::deserialize(&v).map_err(|e| Error::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn iter_edges_with_props<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Error>> + 'a {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
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
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Error>> + 'a {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
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
                    Err(Error::MissingPropertyData)
                }
            })
        })
    }
}

impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned,
    > TripleStoreIntoIter<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn into_iter_nodes(
        self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>,
        impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>,
    ) {
        let node_iter = self.node_props.into_iter().map(|r| match r {
            Ok((k, v)) => {
                let k = Ulid(bincode::deserialize(&k).map_err(|e| Error::SerializationError(e))?);
                let v = bincode::deserialize(&v).map_err(|e| Error::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(Error::SledError(e)),
        });

        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
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
                    .map_err(|e| Error::SledError(e))?
                    .map(|data| {
                        bincode::deserialize(&data).map_err(|e| Error::SerializationError(e))
                    })
                    .transpose();

                if let Some(pred_data) = pred_data? {
                    Ok((k, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            })
        });
        (node_iter, edge_iter)
    }

    fn into_iter_vertices(
        self,
    ) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>> {
        self.node_props.into_iter().map(|r| match r {
            Ok((k, v)) => {
                let k = Ulid(bincode::deserialize(&k).map_err(|e| Error::SerializationError(e))?);
                let v = bincode::deserialize(&v).map_err(|e| Error::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn into_iter_edges_with_props(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>
    {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.into_iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.into_iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.into_iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
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
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(self.spo_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::POS => Box::new(self.pos_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
                        v,
                    ))
                })
            })),
            EdgeOrder::OSP => Box::new(self.osp_data.iter().map(|r| {
                r.map_err(|e| Error::SledError(e)).and_then(|(k, v)| {
                    Ok((
                        Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?),
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
                    Err(Error::MissingPropertyData)
                }
            })
        })
    }
}

// #[cfg(test)]
// mod test {
//     #[test]
//     fn test_iter_spo() {
//         todo!()
//     }

//     #[test]
//     fn test_iter_pos() {
//         todo!()
//     }

//     #[test]
//     fn test_iter_osp() {
//         todo!()
//     }

//     #[test]
//     fn test_iter_node() {
//         todo!()
//     }

//     #[test]
//     fn test_iter_edge_spo() {
//         todo!()
//     }

//     #[test]
//     fn test_iter_edge_pos() {
//         todo!()
//     }

//     #[test]
//     fn test_iter_edge_osp() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iters() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iter_spo() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iter_pos() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iter_osp() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iter_node() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iter_edge_spo() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iter_edge_pos() {
//         todo!()
//     }

//     #[test]
//     fn test_into_iter_edge_osp() {
//         todo!()
//     }
// }
