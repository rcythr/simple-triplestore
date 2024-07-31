use serde::{de::DeserializeOwned, Serialize};
use sled::transaction::{ConflictableTransactionError, Transactional};

use crate::{
    prelude::*,
    traits::{IdType, Mergeable, Property},
    MergeError, Triple,
};

use super::{SledTripleStore, SledTripleStoreError};

impl<
        Id: IdType,
        NodeProps: Property + Mergeable + Serialize + DeserializeOwned,
        EdgeProps: Property + Mergeable + Serialize + DeserializeOwned,
    > TripleStoreMerge<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn merge<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(crate::EdgeOrder::SPO);

        for r in other_nodes {
            let (id, data) = r.map_err(|e| MergeError::Right(e))?;
            self.merge_node(id, data).map_err(|e| MergeError::Left(e))?;
        }

        for r in other_edges {
            let (id, other_edge_props) = r.map_err(|e| MergeError::Right(e))?;
            self.merge_edge(id, other_edge_props)
                .map_err(|e| MergeError::Left(e))?;
        }

        Ok(())
    }

    fn merge_node(&mut self, node: Id, props: NodeProps) -> Result<(), Self::Error> {
        let key_bytes = &node.to_be_bytes();

        (&self.node_props)
            .transaction(|node_props| {
                match node_props.get(key_bytes.as_ref())? {
                    None => {
                        node_props.insert(
                            key_bytes.as_ref(),
                            bincode::serialize(&props).map_err(|e| {
                                ConflictableTransactionError::Abort(
                                    SledTripleStoreError::SerializationError(e),
                                )
                            })?,
                        )?;
                    }

                    Some(existing_value) => {
                        let mut old_props: NodeProps = bincode::deserialize(&existing_value)
                            .map_err(|e| {
                                ConflictableTransactionError::Abort(
                                    SledTripleStoreError::SerializationError(e),
                                )
                            })?;
                        old_props.merge(props.clone());
                        node_props.insert(
                            key_bytes.as_ref(),
                            bincode::serialize(&old_props).map_err(|e| {
                                ConflictableTransactionError::Abort(
                                    SledTripleStoreError::SerializationError(e),
                                )
                            })?,
                        )?;
                    }
                }

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

    fn merge_edge(&mut self, triple: Triple<Id>, props: EdgeProps) -> Result<(), Self::Error> {
        let new_edge_props_id = self.id_generator.fresh().to_be_bytes();
        let spo_triple = Id::encode_spo_triple(&triple);
        let pos_triple = Id::encode_pos_triple(&triple);
        let osp_triple = Id::encode_osp_triple(&triple);

        (
            &self.edge_props,
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
        )
            .transaction(|(edge_props, spo_data, pos_data, osp_data)| {
                let old_edge_props_id =
                    spo_data.insert(spo_triple.as_ref(), new_edge_props_id.as_ref())?;
                pos_data.insert(pos_triple.as_ref(), new_edge_props_id.as_ref())?;
                osp_data.insert(osp_triple.as_ref(), new_edge_props_id.as_ref())?;

                match old_edge_props_id {
                    None => {
                        edge_props.insert(
                            new_edge_props_id.as_ref(),
                            bincode::serialize(&props).map_err(|e| {
                                ConflictableTransactionError::Abort(
                                    SledTripleStoreError::SerializationError(e),
                                )
                            })?,
                        )?;
                    }

                    Some(old_edge_props_id) => {
                        let old_value = edge_props.remove(old_edge_props_id)?;
                        match old_value {
                            None => {
                                edge_props.insert(
                                    new_edge_props_id.as_ref(),
                                    bincode::serialize(&props).map_err(|e| {
                                        ConflictableTransactionError::Abort(
                                            SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )?;
                            }

                            Some(old_value) => {
                                let mut old_props: EdgeProps = bincode::deserialize(&old_value)
                                    .map_err(|e| {
                                        ConflictableTransactionError::Abort(
                                            SledTripleStoreError::SerializationError(e),
                                        )
                                    })?;
                                old_props.merge(props.clone());
                                edge_props.insert(
                                    new_edge_props_id.as_ref(),
                                    bincode::serialize(&old_props).map_err(|e| {
                                        ConflictableTransactionError::Abort(
                                            SledTripleStoreError::SerializationError(e),
                                        )
                                    })?,
                                )?;
                            }
                        }
                    }
                }

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
}

#[cfg(test)]
mod test {
    use crate::{SledTripleStore, UlidIdGenerator};

    #[test]
    fn test_merge() {
        let mut temp_dirs = Vec::new();
        crate::conformance::merge::test_merge(|| {
            let (temp_dir, db) = crate::sled::create_test_db().expect("ok");
            let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
            temp_dirs.push((temp_dir, db));
            sled_db
        });
    }

    #[test]
    fn test_merge_node() {
        let mut temp_dirs = Vec::new();
        crate::conformance::merge::test_merge_node(|| {
            let (temp_dir, db) = crate::sled::create_test_db().expect("ok");
            let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
            temp_dirs.push((temp_dir, db));
            sled_db
        });
    }

    #[test]
    fn test_merge_edge() {
        let mut temp_dirs = Vec::new();
        crate::conformance::merge::test_merge_edge(|| {
            let (temp_dir, db) = crate::sled::create_test_db().expect("ok");
            let sled_db = SledTripleStore::new(&db, UlidIdGenerator::new()).expect("ok");
            temp_dirs.push((temp_dir, db));
            sled_db
        });
    }
}
