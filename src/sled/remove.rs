use std::borrow::Borrow;

use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;
use ulid::Ulid;

use crate::{prelude::*, PropertyType};

use super::Error;
use super::SledTripleStore;

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > SledTripleStore<NodeProperties, EdgeProperties>
{
    // Gets the set of outgoing edges from a given node.
    fn get_spo_edge_range(&self, node: &Ulid) -> Result<(Vec<Triple>, Vec<IVec>), Error> {
        self.spo_data
            .range((
                std::ops::Bound::Included(
                    Triple {
                        sub: node.clone(),
                        pred: Ulid(u128::MIN),
                        obj: Ulid(u128::MIN),
                    }
                    .encode_spo(),
                ),
                std::ops::Bound::Included(
                    Triple {
                        sub: node.clone(),
                        pred: Ulid(u128::MAX),
                        obj: Ulid(u128::MAX),
                    }
                    .encode_spo(),
                ),
            ))
            .try_fold(
                (Vec::new(), Vec::new()),
                |(mut triples, mut edge_data_ids), r| {
                    let (triple, edge_data_id) = r.map_err(|e| Error::SledError(e))?;

                    triples.push(Triple::decode_spo(
                        &triple[..]
                            .try_into()
                            .map_err(|_| super::Error::KeySizeError)?,
                    ));
                    edge_data_ids.push(edge_data_id.clone());
                    Ok((triples, edge_data_ids))
                },
            )
    }

    // Gets the set of incoming edges to a given node.
    fn get_osp_edge_range(&self, node: &Ulid) -> Result<(Vec<Triple>, Vec<IVec>), Error> {
        self.osp_data
            .range((
                std::ops::Bound::Included(
                    Triple {
                        sub: Ulid(u128::MIN),
                        pred: Ulid(u128::MIN),
                        obj: node.clone(),
                    }
                    .encode_osp(),
                ),
                std::ops::Bound::Included(
                    Triple {
                        sub: Ulid(u128::MAX),
                        pred: Ulid(u128::MAX),
                        obj: node.clone(),
                    }
                    .encode_osp(),
                ),
            ))
            .try_fold(
                (Vec::new(), Vec::new()),
                |(mut triples, mut edge_data_ids), r| {
                    let (triple, edge_data_id) = r.map_err(|e| Error::SledError(e))?;
                    triples.push(Triple::decode_osp(
                        &triple[..]
                            .try_into()
                            .map_err(|_| super::Error::KeySizeError)?,
                    ));
                    edge_data_ids.push(edge_data_id.clone());
                    Ok((triples, edge_data_ids))
                },
            )
    }
}

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreRemove<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn remove_node(&mut self, node: impl Borrow<Ulid>) -> Result<(), Self::Error> {
        // Find all uses of this node in the edges.
        let (forward_triples, forward_edge_data_ids) = self.get_spo_edge_range(node.borrow())?;
        let (backward_triples, backward_edge_data_ids) = self.get_osp_edge_range(node.borrow())?;

        // Remove the node props.
        self.node_props
            .remove(node.borrow().0.to_be_bytes())
            .map_err(|e| Error::SledError(e))?;

        // Remove all the edge props for all the edges we'll be removing.
        for edge_data_id in forward_edge_data_ids
            .into_iter()
            .chain(backward_edge_data_ids.into_iter())
        {
            self.edge_props
                .remove(&edge_data_id)
                .map_err(|e| Error::SledError(e))?;
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

    fn remove_edge(&mut self, triple: Triple) -> Result<(), Self::Error> {
        let edge_data_id = self
            .spo_data
            .remove(triple.encode_spo())
            .map_err(|e| Error::SledError(e))?;
        self.pos_data
            .remove(triple.encode_pos())
            .map_err(|e| Error::SledError(e))?;
        self.osp_data
            .remove(triple.encode_osp())
            .map_err(|e| Error::SledError(e))?;

        if let Some(edge_data_id) = edge_data_id {
            self.edge_props
                .remove(edge_data_id)
                .map_err(|e| Error::SledError(e))?;
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
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::remove::test_remove_node(sled_db);
    }

    #[test]
    fn test_remove_edge() {
        let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
        let sled_db = SledTripleStore::new(&db).expect("ok");
        crate::conformance::remove::test_remove_edge(sled_db);
    }
}
