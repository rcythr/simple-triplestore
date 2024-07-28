use serde::{de::DeserializeOwned, Serialize};

use crate::{MemTripleStore, PropertiesType, TripleStoreQuery};

use super::SledTripleStore;

impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned,
    > TripleStoreQuery<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    fn query(&mut self, query: crate::Query) -> Result<Self::QueryResult, Self::Error> {
        match query {
            crate::Query::NodeProperty(_) => todo!(),
            crate::Query::EdgeProperty(_) => todo!(),
            crate::Query::__O(_) => todo!(),
            crate::Query::S__(_) => todo!(),
            crate::Query::_P_(_) => todo!(),
            crate::Query::_PO(_) => todo!(),
            crate::Query::S_O(_) => todo!(),
            crate::Query::SP_(_) => todo!(),
        }
    }
}
