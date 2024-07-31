use serde::{de::DeserializeOwned, Serialize};
use sled::Transactional;

use crate::{
    prelude::*,
    traits::{IdType, Property},
    Triple,
};

use super::{SledTripleStore, SledTripleStoreError};

impl<
        Id: IdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreInsert<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn insert_node(&mut self, node: Id, props: NodeProps) -> Result<(), SledTripleStoreError> {
        let key_bytes = &node.to_be_bytes();
        let data_bytes = bincode::serialize(&props)?;
        self.node_props.insert(key_bytes, data_bytes)?;
        Ok(())
    }

    fn insert_edge(
        &mut self,
        triple: Triple<Id>,
        props: EdgeProps,
    ) -> Result<(), SledTripleStoreError> {
        let prop_key = self.id_generator.fresh();
        let prop_key_bytes = prop_key.to_be_bytes();

        let data_bytes = bincode::serialize(&props)?;

        (
            &self.edge_props,
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
        )
            .transaction(move |(edge_props, spo_data, pos_data, osp_data)| {
                edge_props.insert(prop_key_bytes.as_ref(), data_bytes.as_slice())?;
                spo_data.insert(
                    Id::encode_spo_triple(&triple).as_ref(),
                    prop_key_bytes.as_ref(),
                )?;
                pos_data.insert(
                    Id::encode_pos_triple(&triple).as_ref(),
                    prop_key_bytes.as_ref(),
                )?;
                osp_data.insert(
                    Id::encode_osp_triple(&triple).as_ref(),
                    prop_key_bytes.as_ref(),
                )?;
                Ok(())
            })
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(e) => SledTripleStoreError::SledError(e),
                sled::transaction::TransactionError::Storage(e) => {
                    SledTripleStoreError::SledError(e)
                }
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{SledTripleStore, UlidIdGenerator};

    #[test]
    fn test_insert_node() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::insert::test_insert_node(sled_db);
    }

    #[test]
    fn test_insert_edge() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::insert::test_insert_edge(sled_db);
    }
}
