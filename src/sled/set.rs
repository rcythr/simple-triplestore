use serde::{de::DeserializeOwned, Serialize};

use crate::{
    MemTripleStore, TripleStoreExtend, TripleStoreInsert, TripleStoreIntoIter, TripleStoreSetOps,
};

use super::SledTripleStore;

impl<
        NodeProperties: Clone + Serialize + DeserializeOwned,
        EdgeProperties: Clone + Serialize + DeserializeOwned,
    > TripleStoreSetOps<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    type SetOpsResult = MemTripleStore<NodeProperties, EdgeProperties>;

    fn union(self, other: Self) -> Result<Self::SetOpsResult, Self::Error> {
        let mut result = MemTripleStore::new();

        for r in self
            .node_props
            .into_iter()
            .chain(other.node_props.into_iter())
        {
            let (id, props) = r.map_err(|e| Self::Error::SledError(e))?;

            let id = bincode::deserialize(&id).map_err(|e| Self::Error::SerializationError(e))?;
            let props =
                bincode::deserialize(&props).map_err(|e| Self::Error::SerializationError(e))?;

            result.insert_node(id, props).expect("success");
        }

        for r in self.into_iter_edge_spo().chain(other.into_iter_edge_spo()) {
            let (triple, props) = r?;

            result.insert_edge(triple, props).expect("success");
        }

        Ok(result)
    }

    fn intersection(self, other: Self) -> Result<Self::SetOpsResult, Self::Error> {
        todo!()
    }

    fn difference(self, other: Self) -> Result<Self::SetOpsResult, Self::Error> {
        todo!()
    }
}
