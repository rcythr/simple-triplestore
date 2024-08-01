use serde::{de::DeserializeOwned, Serialize};

use ulid::Ulid;

use crate::{
    prelude::*,
    traits::{ConcreteIdType, Property},
    ExtendError,
};

use super::{SledTripleStore, SledTripleStoreError};

impl<
        Id: ConcreteIdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreExtend<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn extend<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), crate::traits::ExtendError<SledTripleStoreError, E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(crate::EdgeOrder::SPO);

        for r in other_nodes {
            let (id, data) = r.map_err(|e| ExtendError::Right(e))?;
            let data = bincode::serialize(&data).map_err(|e| ExtendError::Left(e.into()))?;
            self.node_props
                .update_and_fetch(&id.to_be_bytes(), |_| Some(data.clone()))
                .map_err(|e| ExtendError::Left(e.into()))?;
        }

        for r in other_edges {
            let (id, other_edge_props) = r.map_err(|e| ExtendError::Right(e))?;

            // Serialize and insert the new data along with a fresh id for it.
            let other_edge_props_id = &Ulid::new().0.to_be_bytes();
            let other_edge_props =
                bincode::serialize(&other_edge_props).map_err(|e| ExtendError::Left(e.into()))?;
            self.edge_props
                .insert(other_edge_props_id.as_slice(), other_edge_props)
                .map_err(|e| ExtendError::Left(e.into()))?;

            // Update the edge tables
            let old_edge_props_id = self
                .spo_data
                .fetch_and_update(Id::encode_spo_triple(&id), |_| {
                    Some(other_edge_props_id.as_slice())
                })
                .map_err(|e| ExtendError::Left(e.into()))?;
            self.pos_data
                .insert(Id::encode_pos_triple(&id), other_edge_props_id.as_slice())
                .map_err(|e| ExtendError::Left(e.into()))?;
            self.osp_data
                .insert(Id::encode_osp_triple(&id), other_edge_props_id)
                .map_err(|e| ExtendError::Left(e.into()))?;

            old_edge_props_id
                .map(|old_edge_props_id| self.edge_props.remove(old_edge_props_id))
                .transpose()
                .map_err(|e| ExtendError::Left(e.into()))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{SledTripleStore, UlidIdGenerator};

    #[test]
    fn test_extend() {
        let (_left_tempdir, left_db) = crate::sled::create_test_db().expect("ok");
        let (_right_tempdir, right_db) = crate::sled::create_test_db().expect("ok");

        let left = SledTripleStore::new(&left_db, UlidIdGenerator::new()).expect("ok");
        let right = SledTripleStore::new(&right_db, UlidIdGenerator::new()).expect("ok");

        crate::conformance::extend::test_extend(left, right);
    }
}
