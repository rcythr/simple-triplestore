use serde::{de::DeserializeOwned, Serialize};

use crate::{
    MemTripleStore, PropertiesType, TripleStoreInsert, TripleStoreIntoIter, TripleStoreSetOps,
};

use super::SledTripleStore;

impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned + PartialEq,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned + PartialEq,
    > TripleStoreSetOps<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    type SetOpsResult = MemTripleStore<NodeProperties, EdgeProperties>;

    fn union(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties>,
    ) -> Result<Self::SetOpsResult, Self::Error> {
        let mut result = MemTripleStore::new();

        // Pull in node and edge content from self.
        for r in self.node_props.into_iter() {
            let (id, props) = r.map_err(|e| Self::Error::SledError(e))?;

            let id = bincode::deserialize(&id).map_err(|e| Self::Error::SerializationError(e))?;
            let props =
                bincode::deserialize(&props).map_err(|e| Self::Error::SerializationError(e))?;

            result.insert_node(id, props).expect("success");
        }

        for r in self.into_iter_edge_spo() {
            let (triple, props) = r.map_err(|_| Self::Error::SetOpsFailure)?;
            result.insert_edge(triple, props).expect("success");
        }

        // Pull in node and edge content from other.
        let (node_iter, edge_iter) = other.into_iters();
        for r in node_iter {
            let (id, props) = r.map_err(|_| Self::Error::SetOpsFailure)?;
            result.insert_node(id, props).expect("success");
        }

        for r in edge_iter {
            let (triple, props) = r.map_err(|_| Self::Error::SetOpsFailure)?;
            result.insert_edge(triple, props).expect("success");
        }

        Ok(result)
    }

    fn intersection(self, _other: Self) -> Result<Self::SetOpsResult, Self::Error> {
        todo!()
    }

    fn difference(self, _other: Self) -> Result<Self::SetOpsResult, Self::Error> {
        todo!()
    }
}
