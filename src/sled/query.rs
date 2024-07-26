use serde::{de::DeserializeOwned, Serialize};

use crate::{MemTripleStore, TripleStoreQuery};

use super::SledTripleStore;

impl<
        NodeProperties: Clone + Serialize + DeserializeOwned,
        EdgeProperties: Clone + Serialize + DeserializeOwned,
    > TripleStoreQuery<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    fn query(&mut self, query: crate::Query) -> Result<Self::QueryResult, Self::Error> {
        todo!()
    }
}
