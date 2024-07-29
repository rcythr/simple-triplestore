use serde::{de::DeserializeOwned, Serialize};
use sled::Transactional;
use ulid::Ulid;

use crate::{PropertyType, Triple, TripleStoreInsert};

use super::{Error, SledTripleStore};

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreInsert<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn insert_node(&mut self, node: Ulid, props: NodeProperties) -> Result<(), Error> {
        let key_bytes = bincode::serialize(&node.0).map_err(|e| Error::SerializationError(e))?;
        let data_bytes = bincode::serialize(&props).map_err(|e| Error::SerializationError(e))?;
        self.node_props
            .insert(key_bytes, data_bytes)
            .map_err(|e| Error::SledError(e))?;
        Ok(())
    }

    fn insert_node_batch<I: IntoIterator<Item = (Ulid, NodeProperties)>>(
        &mut self,
        nodes: I,
    ) -> Result<(), Error> {
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

    fn insert_edge(&mut self, triple: Triple, props: EdgeProperties) -> Result<(), Error> {
        let prop_key = Ulid::new();
        let prop_key_bytes =
            bincode::serialize(&prop_key.0).map_err(|e| Error::SerializationError(e))?;

        let data_bytes = bincode::serialize(&props).map_err(|e| Error::SerializationError(e))?;

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

    fn insert_edge_batch<I: IntoIterator<Item = (Triple, EdgeProperties)>>(
        &mut self,
        triples: I,
    ) -> Result<(), Error> {
        let triples = triples
            .into_iter()
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
}

// #[cfg(test)]
// mod test {
//     #[test]
//     fn test_insert_node() {
//         todo!()
//     }

//     #[test]
//     fn test_insert_node_batch() {
//         todo!()
//     }

//     #[test]
//     fn test_insert_edge() {
//         todo!()
//     }

//     #[test]
//     fn test_insert_edge_batch() {
//         todo!()
//     }
// }
