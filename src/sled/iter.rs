use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;
use ulid::Ulid;

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
    fn iter_spo<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Error>> + 'a {
        self.spo_data.iter().map(|r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?);
                self.iter_impl(triple, v)
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn iter_pos<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Error>> + 'a {
        self.pos_data.iter().map(|r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?);
                self.iter_impl(triple, v)
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn iter_osp<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Error>> + 'a {
        self.osp_data.iter().map(|r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?);
                self.iter_impl(triple, v)
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn iter_node<'a>(&'a self) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Error>> {
        self.node_props.iter().map(|r| match r {
            Ok((k, v)) => {
                let k = Ulid(bincode::deserialize(&k).map_err(|e| Error::SerializationError(e))?);
                let v = bincode::deserialize(&v).map_err(|e| Error::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn iter_edge_spo<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Error>> + 'a {
        self.spo_data.iter().map(|r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?);

                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((triple, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn iter_edge_pos<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Error>> + 'a {
        self.pos_data.iter().map(|r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?);

                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((triple, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn iter_edge_osp<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Error>> + 'a {
        self.osp_data.iter().map(|r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?);

                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((triple, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }
}

impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned,
    > TripleStoreIntoIter<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn into_iters(
        self,
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
        let edge_iter = self.spo_data.into_iter().map(move |r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?);

                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((triple, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            }
            Err(e) => Err(Error::SledError(e)),
        });
        (node_iter, edge_iter)
    }

    fn into_iter_spo(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>
    {
        self.spo_data.into_iter().map(move |r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?);
                self.iter_impl(triple, v)
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn into_iter_pos(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>
    {
        self.pos_data.into_iter().map(move |r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?);
                self.iter_impl(triple, v)
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn into_iter_osp(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>
    {
        self.osp_data.into_iter().map(move |r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?);
                self.iter_impl(triple, v)
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn into_iter_node(self) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>> {
        self.node_props.into_iter().map(|r| match r {
            Ok((k, v)) => {
                let k = Ulid(bincode::deserialize(&k).map_err(|e| Error::SerializationError(e))?);
                let v = bincode::deserialize(&v).map_err(|e| Error::SerializationError(e))?;
                Ok((k, v))
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn into_iter_edge_spo(
        self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> {
        self.spo_data.into_iter().map(move |r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_spo(&k[..].try_into().map_err(|_| Error::KeySizeError)?);

                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((triple, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn into_iter_edge_pos(
        self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> {
        self.pos_data.into_iter().map(move |r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_pos(&k[..].try_into().map_err(|_| Error::KeySizeError)?);

                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((triple, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }

    fn into_iter_edge_osp(
        self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> {
        self.osp_data.into_iter().map(move |r| match r {
            Ok((k, v)) => {
                let triple =
                    Triple::decode_osp(&k[..].try_into().map_err(|_| Error::KeySizeError)?);

                if let Some(pred_data) = self.get_edge_data_internal(&v)? {
                    Ok((triple, pred_data))
                } else {
                    Err(Error::MissingPropertyData)
                }
            }
            Err(e) => Err(Error::SledError(e)),
        })
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_iter_spo() {
        todo!()
    }

    #[test]
    fn test_iter_pos() {
        todo!()
    }

    #[test]
    fn test_iter_osp() {
        todo!()
    }

    #[test]
    fn test_iter_node() {
        todo!()
    }

    #[test]
    fn test_iter_edge_spo() {
        todo!()
    }

    #[test]
    fn test_iter_edge_pos() {
        todo!()
    }

    #[test]
    fn test_iter_edge_osp() {
        todo!()
    }

    #[test]
    fn test_into_iters() {
        todo!()
    }

    fn test_into_iter_spo() {
        todo!()
    }

    #[test]
    fn test_into_iter_pos() {
        todo!()
    }

    #[test]
    fn test_into_iter_osp() {
        todo!()
    }

    #[test]
    fn test_into_iter_node() {
        todo!()
    }

    #[test]
    fn test_into_iter_edge_spo() {
        todo!()
    }

    #[test]
    fn test_into_iter_edge_pos() {
        todo!()
    }

    #[test]
    fn test_into_iter_edge_osp() {
        todo!()
    }
}
