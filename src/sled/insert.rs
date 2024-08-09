use serde::{de::DeserializeOwned, Serialize};
use sled::Transactional;

use crate::{
    prelude::*,
    traits::{ConcreteIdType, Property, TripleStoreInsertBatch},
    Triple,
};

use super::{SledTripleStore, SledTripleStoreError};

impl<
        Id: ConcreteIdType,
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

impl<
        Id: ConcreteIdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreInsertBatch<Id, NodeProps, EdgeProps>
    for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn insert_batch<T, U>(&mut self, nodes: T, edges: U) -> Result<(), Self::Error>
    where
        T: Iterator<Item = (Id, NodeProps)>,
        U: Iterator<Item = (Triple<Id>, EdgeProps)>,
    {
        // Insert the Nodes
        {
            let mut node_props_batch = sled::Batch::default();
            for (node, props) in nodes {
                let key_bytes = &node.to_be_bytes();
                let data_bytes = bincode::serialize(&props)?;
                node_props_batch.insert(key_bytes.as_ref(), data_bytes);
            }
            self.node_props.apply_batch(node_props_batch)?;
        }

        // Insert the Edges
        {
            let mut edge_props_batch = sled::Batch::default();
            let mut spo_batch = sled::Batch::default();
            let mut pos_batch = sled::Batch::default();
            let mut osp_batch = sled::Batch::default();
            for (triple, props) in edges {
                let prop_key = self.id_generator.fresh();
                let prop_key_bytes = prop_key.to_be_bytes();

                let data_bytes = bincode::serialize(&props)?;

                edge_props_batch.insert(prop_key_bytes.as_ref(), data_bytes.as_slice());
                spo_batch.insert(
                    Id::encode_spo_triple(&triple).as_ref(),
                    prop_key_bytes.as_ref(),
                );
                pos_batch.insert(
                    Id::encode_pos_triple(&triple).as_ref(),
                    prop_key_bytes.as_ref(),
                );
                osp_batch.insert(
                    Id::encode_osp_triple(&triple).as_ref(),
                    prop_key_bytes.as_ref(),
                );
            }

            self.edge_props.apply_batch(edge_props_batch)?;
            self.spo_data.apply_batch(spo_batch)?;
            self.pos_data.apply_batch(pos_batch)?;
            self.osp_data.apply_batch(osp_batch)?;
        }

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
