use std::borrow::Borrow;

use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;

use crate::traits::IdType;
use crate::{prelude::*, Property};

use super::SledTripleStore;
use super::SledTripleStoreError;

impl<
        Id: IdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > SledTripleStore<Id, NodeProps, EdgeProps>
{
    // Gets the set of outgoing edges from a given node.
    fn get_spo_edge_range(
        &self,
        node: &Id,
    ) -> Result<(Vec<Triple<Id>>, Vec<IVec>), SledTripleStoreError> {
        self.spo_data
            .range(Id::key_bounds_1(node.clone()))
            .try_fold(
                (Vec::new(), Vec::new()),
                |(mut triples, mut edge_data_ids), r| {
                    let (triple, edge_data_id) =
                        r.map_err(|e| SledTripleStoreError::SledError(e))?;

                    triples.push(Id::decode_spo_triple(
                        &triple[..]
                            .try_into()
                            .map_err(|_| super::SledTripleStoreError::KeySizeError)?,
                    ));
                    edge_data_ids.push(edge_data_id.clone());
                    Ok((triples, edge_data_ids))
                },
            )
    }

    // Gets the set of incoming edges to a given node.
    fn get_osp_edge_range(
        &self,
        node: &Id,
    ) -> Result<(Vec<Triple<Id>>, Vec<IVec>), SledTripleStoreError> {
        self.osp_data
            .range(Id::key_bounds_1(node.clone()))
            .try_fold(
                (Vec::new(), Vec::new()),
                |(mut triples, mut edge_data_ids), r| {
                    let (triple, edge_data_id) =
                        r.map_err(|e| SledTripleStoreError::SledError(e))?;
                    triples.push(Id::decode_osp_triple(
                        &triple[..]
                            .try_into()
                            .map_err(|_| super::SledTripleStoreError::KeySizeError)?,
                    ));
                    edge_data_ids.push(edge_data_id.clone());
                    Ok((triples, edge_data_ids))
                },
            )
    }
}

impl<
        Id: IdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreRemove<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn remove_node(&mut self, node: impl Borrow<Id>) -> Result<(), Self::Error> {
        // Find all uses of this node in the edges.
        let (forward_triples, forward_edge_data_ids) = self.get_spo_edge_range(node.borrow())?;
        let (backward_triples, backward_edge_data_ids) = self.get_osp_edge_range(node.borrow())?;

        // Remove the node props.
        self.node_props
            .remove(node.borrow().to_be_bytes())
            .map_err(|e| SledTripleStoreError::SledError(e))?;

        // Remove all the edge props for all the edges we'll be removing.
        for edge_data_id in forward_edge_data_ids
            .into_iter()
            .chain(backward_edge_data_ids.into_iter())
        {
            self.edge_props
                .remove(&edge_data_id)
                .map_err(|e| SledTripleStoreError::SledError(e))?;
        }

        // Remove the forward and backward edges
        for edge in forward_triples
            .into_iter()
            .chain(backward_triples.into_iter())
        {
            self.remove_edge(edge)?;
        }

        Ok(())
    }

    fn remove_edge(&mut self, triple: Triple<Id>) -> Result<(), Self::Error> {
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
