use serde::{de::DeserializeOwned, Serialize};
use sled::Transactional;

use crate::TripleStoreExtend;

use super::{Error, SledTripleStore};

impl<
        NodeProperties: Clone + Serialize + DeserializeOwned,
        EdgeProperties: Clone + Serialize + DeserializeOwned,
    > TripleStoreExtend<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn extend(&mut self, other: Self) -> Result<(), Error> {
        (
            &self.node_props,
            &self.edge_props,
            &self.spo_data,
            &self.pos_data,
            &self.osp_data,
        )
            .transaction(
                move |(node_props, edge_props, spo_data, pos_data, osp_data)| {
                    for r in other.node_props.into_iter() {
                        let (k, v) = r?;
                        node_props.insert(k, v)?;
                    }

                    for r in other.edge_props.into_iter() {
                        let (k, v) = r?;
                        edge_props.insert(k, v)?;
                    }

                    for r in other.spo_data.into_iter() {
                        let (k, v) = r?;
                        spo_data.insert(k, v)?;
                    }

                    for r in other.pos_data.into_iter() {
                        let (k, v) = r?;
                        pos_data.insert(k, v)?;
                    }

                    for r in other.osp_data.into_iter() {
                        let (k, v) = r?;
                        osp_data.insert(k, v)?;
                    }

                    Ok(())
                },
            )
            .map_err(|e| match e {
                sled::transaction::TransactionError::Abort(e) => Error::SledError(e),
                sled::transaction::TransactionError::Storage(e) => Error::SledError(e),
            })?;
        Ok(())
    }
}
