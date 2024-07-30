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
        let key_bytes = &node.0.to_be_bytes();
        let data_bytes = bincode::serialize(&props).map_err(|e| Error::SerializationError(e))?;
        self.node_props
            .insert(key_bytes, data_bytes)
            .map_err(|e| Error::SledError(e))?;
        Ok(())
    }

    fn insert_edge(&mut self, triple: Triple, props: EdgeProperties) -> Result<(), Error> {
        let prop_key = Ulid::new();
        let prop_key_bytes = &prop_key.0.to_be_bytes();

        let data_bytes = bincode::serialize(&props).map_err(|e| Error::SerializationError(e))?;

        (
            &self.edge_props,
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
        )
            .transaction(move |(edge_props, spo_data, pos_data, osp_data)| {
                edge_props.insert(prop_key_bytes.as_slice(), data_bytes.as_slice())?;
                spo_data.insert(triple.encode_spo().as_slice(), prop_key_bytes.as_slice())?;
                pos_data.insert(triple.encode_pos().as_slice(), prop_key_bytes.as_slice())?;
                osp_data.insert(triple.encode_osp().as_slice(), prop_key_bytes.as_slice())?;
                Ok(())
            })
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(e) => Error::SledError(e),
                sled::transaction::TransactionError::Storage(e) => Error::SledError(e),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_insert_node() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::insert::test_insert_node(sled_db);
    }

    #[test]
    fn test_insert_edge() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::insert::test_insert_edge(sled_db);
    }
}
