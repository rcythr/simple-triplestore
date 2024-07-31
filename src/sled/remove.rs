use std::borrow::Borrow;

use serde::{de::DeserializeOwned, Serialize};
use sled::Batch;
use sled::IVec;
use sled::Transactional;

use crate::traits::IdType;
use crate::{prelude::*, Property};

use super::SledTripleStore;
use super::SledTripleStoreError;

impl<
        Id: IdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreRemove<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn remove_node(&mut self, node: impl Borrow<Id>) -> Result<(), Self::Error> {
        // Collect forward edges from this node as subject.
        let (spo_forward_batch, pos_forward_batch, osp_forward_batch, edge_props_forward_batch) =
            self.spo_data
                .range(Id::key_bounds_1(node.borrow().clone()))
                .try_fold(
                    (
                        Batch::default(),
                        Batch::default(),
                        Batch::default(),
                        Batch::default(),
                    ),
                    |(mut spo_batch, mut pos_batch, mut osp_batch, mut edge_data_ids), r| {
                        let (spo_triple, edge_data_id) =
                            r.map_err(|e| SledTripleStoreError::SledError(e))?;

                        let triple = Id::decode_spo_triple(
                            &spo_triple[..]
                                .try_into()
                                .map_err(|_| super::SledTripleStoreError::KeySizeError)?,
                        );

                        spo_batch.remove(&spo_triple);
                        pos_batch.remove(Id::encode_pos_triple(&triple).as_ref());
                        osp_batch.remove(Id::encode_osp_triple(&triple).as_ref());
                        edge_data_ids.remove(edge_data_id.as_ref());

                        Ok::<(Batch, Batch, Batch, Batch), SledTripleStoreError>((
                            spo_batch,
                            pos_batch,
                            osp_batch,
                            edge_data_ids,
                        ))
                    },
                )?;

        // Collect backward edges from this node as object.
        let (spo_backward_batch, pos_backward_batch, osp_backward_batch, edge_props_backward_batch) =
            self.osp_data
                .range(Id::key_bounds_1(node.borrow().clone()))
                .try_fold(
                    (
                        Batch::default(),
                        Batch::default(),
                        Batch::default(),
                        Batch::default(),
                    ),
                    |(mut spo_batch, mut pos_batch, mut osp_batch, mut edge_data_ids), r| {
                        let (osp_triple, edge_data_id) =
                            r.map_err(|e| SledTripleStoreError::SledError(e))?;

                        let triple = Id::decode_osp_triple(
                            &osp_triple[..]
                                .try_into()
                                .map_err(|_| super::SledTripleStoreError::KeySizeError)?,
                        );

                        osp_batch.remove(&osp_triple);
                        pos_batch.remove(Id::encode_pos_triple(&triple).as_ref());
                        spo_batch.remove(Id::encode_spo_triple(&triple).as_ref());
                        edge_data_ids.remove(edge_data_id.as_ref());

                        Ok::<(Batch, Batch, Batch, Batch), SledTripleStoreError>((
                            spo_batch,
                            pos_batch,
                            osp_batch,
                            edge_data_ids,
                        ))
                    },
                )?;

        // Remove the NodeProps, EdgeProps, and edges in one transaction.
        (
            &self.node_props,
            &self.edge_props,
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
        )
            .transaction(|(node_props, edge_props, spo_data, pos_data, osp_data)| {
                node_props.remove(node.borrow().to_be_bytes().as_ref())?;

                edge_props.apply_batch(&edge_props_forward_batch)?;
                edge_props.apply_batch(&edge_props_backward_batch)?;

                spo_data.apply_batch(&spo_forward_batch)?;
                spo_data.apply_batch(&spo_backward_batch)?;

                pos_data.apply_batch(&pos_forward_batch)?;
                pos_data.apply_batch(&pos_backward_batch)?;

                osp_data.apply_batch(&osp_forward_batch)?;
                osp_data.apply_batch(&osp_backward_batch)?;

                Ok(())
            })
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(e) => e,
                sled::transaction::TransactionError::Storage(e) => {
                    SledTripleStoreError::SledError(e)
                }
            })?;

        Ok(())
    }

    fn remove_edge(&mut self, triple: Triple<Id>) -> Result<(), Self::Error> {
        let spo_triple = Id::encode_spo_triple(&triple);
        let pos_triple = Id::encode_pos_triple(&triple);
        let osp_triple = Id::encode_osp_triple(&triple);

        (
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
            &self.edge_props,
        )
            .transaction(|(spo_data, pos_data, osp_data, edge_props)| {
                let edge_props_id = spo_data.remove(spo_triple.as_ref())?;
                pos_data.remove(pos_triple.as_ref())?;
                osp_data.remove(osp_triple.as_ref())?;
                if let Some(edge_props_id) = edge_props_id {
                    edge_props.remove(edge_props_id)?;
                }
                Ok(())
            })
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(e) => e,
                sled::transaction::TransactionError::Storage(e) => {
                    SledTripleStoreError::SledError(e)
                }
            })?;

        let edge_data_id = self
            .spo_data
            .remove(Id::encode_spo_triple(&triple))
            .map_err(|e| SledTripleStoreError::SledError(e))?;
        self.pos_data
            .remove(Id::encode_pos_triple(&triple))
            .map_err(|e| SledTripleStoreError::SledError(e))?;
        self.osp_data
            .remove(Id::encode_osp_triple(&triple))
            .map_err(|e| SledTripleStoreError::SledError(e))?;

        if let Some(edge_data_id) = edge_data_id {
            self.edge_props
                .remove(edge_data_id)
                .map_err(|e| SledTripleStoreError::SledError(e))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_remove_node() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::remove::test_remove_node(sled_db);
    }

    #[test]
    fn test_remove_edge() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
        crate::conformance::remove::test_remove_edge(sled_db);
    }
}
