use ulid::Ulid;

use crate::{MemTripleStore, Triple, TripleStore};
use serde::{de::DeserializeOwned, Serialize};
use sled::Transactional;

pub enum Error {
    SledError(sled::Error),
    SerializationError(bincode::Error),
}

/// A triplestore which is backed by a Sled database.
pub struct SledTripleStore<
    NodeProperties: Serialize + DeserializeOwned,
    EdgeProperties: Serialize + DeserializeOwned,
> {
    _phantom: std::marker::PhantomData<(NodeProperties, EdgeProperties)>,
    node_props: sled::Tree,
    edge_props: sled::Tree,
    spo_data: sled::Tree,
    pos_data: sled::Tree,
    osp_data: sled::Tree,
}

impl<
        NodeProperties: Serialize + DeserializeOwned,
        EdgeProperties: Serialize + DeserializeOwned,
    > SledTripleStore<NodeProperties, EdgeProperties>
{
    pub fn new(db: &sled::Db) -> Result<Self, sled::Error> {
        let node_data = db.open_tree(b"node_data")?;
        let edge_data = db.open_tree(b"edge_data")?;
        let spo_data = db.open_tree(b"spo_data")?;
        let pos_data = db.open_tree(b"pos_data")?;
        let osp_data = db.open_tree(b"osp_data")?;

        Ok(Self {
            node_props: node_data,
            edge_props: edge_data,
            spo_data,
            pos_data,
            osp_data,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<
        NodeProperties: Serialize + DeserializeOwned,
        EdgeProperties: Serialize + DeserializeOwned,
    > SledTripleStore<NodeProperties, EdgeProperties>
{
    // fn iter_spo(&self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     todo!()
    // }

    // fn iter_pos(&self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     todo!()
    // }

    // fn iter_osp(&self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     todo!()
    // }

    // fn iter_node(&self) -> impl Iterator<Item = (Ulid, NodeProperties)> {
    //     todo!()
    // }

    // fn iter_edge_spo(&self) -> impl Iterator<Item = (Triple, EdgeProperties)> {
    //     todo!()
    // }

    // fn iter_edge_pos(&self) -> impl Iterator<Item = (Triple, EdgeProperties)> {
    //     todo!()
    // }

    // fn iter_edge_osp(&self) -> impl Iterator<Item = (Triple, EdgeProperties)> {
    //     todo!()
    // }
}

impl<
        NodeProperties: Clone + Serialize + DeserializeOwned,
        EdgeProperties: Clone + Serialize + DeserializeOwned,
    > TripleStore<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    type Error = Error;

    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;

    fn extend(&mut self, other: Self) {
        todo!()
    }

    // fn into_iter_spo(self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     todo!()
    // }

    // fn into_iter_pos(self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     todo!()
    // }

    // fn into_iter_osp(self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     todo!()
    // }

    // fn into_iter_node(self) -> impl Iterator<Item = (ulid::Ulid, NodeProperties)> {
    //     todo!()
    // }

    // fn into_iter_edge_spo(self) -> impl Iterator<Item = (crate::Triple, EdgeProperties)> {
    //     todo!()
    // }

    // fn into_iter_edge_pos(self) -> impl Iterator<Item = (crate::Triple, EdgeProperties)> {
    //     todo!()
    // }

    // fn into_iter_edge_osp(self) -> impl Iterator<Item = (crate::Triple, EdgeProperties)> {
    //     todo!()
    // }

    fn insert_node(&mut self, node: ulid::Ulid, data: NodeProperties) -> Result<(), Self::Error> {
        let key_bytes = bincode::serialize(&node.0).map_err(|e| Error::SerializationError(e))?;
        let data_bytes = bincode::serialize(&data).map_err(|e| Error::SerializationError(e))?;
        self.node_props
            .insert(key_bytes, data_bytes)
            .map_err(|e| Error::SledError(e))?;
        Ok(())
    }

    fn insert_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (ulid::Ulid, NodeProperties)>,
    ) -> Result<(), Self::Error> {
        let mut batch = sled::Batch::default();
        for (node, data) in nodes {
            let key_bytes =
                bincode::serialize(&node.0).map_err(|e| Error::SerializationError(e))?;
            let data_bytes = bincode::serialize(&data).map_err(|e| Error::SerializationError(e))?;
            batch.insert(key_bytes, data_bytes);
        }
        self.node_props
            .apply_batch(batch)
            .map_err(|e| Error::SledError(e))?;
        Ok(())
    }

    fn insert_edge(
        &mut self,
        triple: crate::Triple,
        data: EdgeProperties,
    ) -> Result<(), Self::Error> {
        let prop_key = Ulid::new();
        let prop_key_bytes =
            bincode::serialize(&prop_key.0).map_err(|e| Error::SerializationError(e))?;

        let data_bytes = bincode::serialize(&data).map_err(|e| Error::SerializationError(e))?;

        (
            &self.edge_props,
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
        )
            .transaction(move |(edge_props, spo_data, pos_data, osp_data)| {
                edge_props.insert(prop_key_bytes.clone(), data_bytes.clone())?;

                spo_data.insert(triple.encode_spo().as_slice(), prop_key_bytes.clone())?;

                pos_data.insert(triple.encode_pos().as_slice(), prop_key_bytes.clone())?;

                osp_data.insert(triple.encode_osp().as_slice(), prop_key_bytes.clone())?;
                Ok(())
            })
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(e) => Error::SledError(e),
                sled::transaction::TransactionError::Storage(e) => Error::SledError(e),
            })?;

        Ok(())
    }

    fn insert_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (crate::Triple, EdgeProperties)>,
    ) -> Result<(), Self::Error> {
        let triples = triples
            .map(|(triple, data)| {
                let prop_key = Ulid::new();
                let prop_key_bytes =
                    bincode::serialize(&prop_key.0).map_err(|e| Error::SerializationError(e))?;
                let data_bytes =
                    bincode::serialize(&data).map_err(|e| Error::SerializationError(e))?;
                Ok((triple, prop_key_bytes, data_bytes))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        (
            &self.edge_props,
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
        )
            .transaction(move |(edge_props, spo_data, pos_data, osp_data)| {
                for (triple, prop_key_bytes, data_bytes) in triples.iter() {
                    edge_props.insert(prop_key_bytes.clone(), data_bytes.clone())?;
                    spo_data.insert(triple.encode_spo().as_slice(), prop_key_bytes.clone())?;
                    pos_data.insert(triple.encode_pos().as_slice(), prop_key_bytes.clone())?;
                    osp_data.insert(triple.encode_osp().as_slice(), prop_key_bytes.clone())?;
                }
                Ok(())
            })
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(e) => Error::SledError(e),
                sled::transaction::TransactionError::Storage(e) => Error::SledError(e),
            })?;

        Ok(())
    }

    fn remove_node(&mut self, node: &ulid::Ulid) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_node_batch(
        &mut self,
        nodes: impl Iterator<Item = ulid::Ulid>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_edge(&mut self, triple: crate::Triple) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_edge_batch(
        &mut self,
        triples: impl Iterator<Item = crate::Triple>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn query(&mut self, query: crate::Query) -> Result<Self::QueryResult, Self::Error> {
        todo!()
    }
}
