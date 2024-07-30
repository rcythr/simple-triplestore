use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

use super::Error;
use crate::{prelude::*, PropertyType};

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreExtend<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn extend<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), ExtendError<Self::Error, E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(crate::EdgeOrder::SPO);

        for r in other_nodes {
            let (id, data) = r.map_err(|e| ExtendError::Right(e))?;
            let data = bincode::serialize(&data)
                .map_err(|e| ExtendError::Left(Error::SerializationError(e)))?;
            self.node_props
                .update_and_fetch(&id.0.to_be_bytes(), |_| Some(data.clone()))
                .map_err(|e| ExtendError::Left(Error::SledError(e)))?;
        }

        for r in other_edges {
            let (id, other_edge_props) = r.map_err(|e| ExtendError::Right(e))?;

            // Serialize and insert the new data along with a fresh id for it.
            let other_edge_props_id = &Ulid::new().0.to_be_bytes();
            let other_edge_props = bincode::serialize(&other_edge_props)
                .map_err(|e| ExtendError::Left(Error::SerializationError(e)))?;
            self.edge_props
                .insert(other_edge_props_id.as_slice(), other_edge_props)
                .map_err(|e| ExtendError::Left(Error::SledError(e)))?;

            // Update the edge tables
            let old_edge_props_id = self
                .spo_data
                .fetch_and_update(&id.encode_spo(), |_| Some(other_edge_props_id.as_slice()))
                .map_err(|e| ExtendError::Left(Error::SledError(e)))?;
            self.pos_data
                .insert(&id.encode_pos(), other_edge_props_id.as_slice())
                .map_err(|e| ExtendError::Left(Error::SledError(e)))?;
            self.osp_data
                .insert(&id.encode_osp(), other_edge_props_id)
                .map_err(|e| ExtendError::Left(Error::SledError(e)))?;

            old_edge_props_id
                .map(|old_edge_props_id| self.edge_props.remove(old_edge_props_id))
                .transpose()
                .map_err(|e| ExtendError::Left(Error::SledError(e)))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::SledTripleStore;

    #[test]
    fn test_extend() {
        let (_left_tempdir, left_db) = crate::sled::create_test_db().expect("ok");
        let (_right_tempdir, right_db) = crate::sled::create_test_db().expect("ok");

        let left = SledTripleStore::new(&left_db).expect("ok");
        let right = SledTripleStore::new(&right_db).expect("ok");

        crate::conformance::extend::test_extend(left, right);
    }
}
