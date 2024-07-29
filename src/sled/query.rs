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
    fn query(&self, query: crate::Query) -> Result<Self::QueryResult, Self::Error> {
        match query {
            crate::Query::NodeProps(_) => todo!(),
            crate::Query::EdgeProps(_) => todo!(),
            crate::Query::O(_) => todo!(),
            crate::Query::S(_) => todo!(),
            crate::Query::P(_) => todo!(),
            crate::Query::PO(_) => todo!(),
            crate::Query::SO(_) => todo!(),
            crate::Query::SP(_) => todo!(),
        }
    }
}
#[cfg(test)]
mod test {
    #[test]
    fn test_query_node_props() {
        todo!()
    }

    #[test]
    fn test_query_edge_props() {
        todo!()
    }

    #[test]
    fn test_query_s() {
        todo!()
    }

    #[test]
    fn test_query_sp() {
        todo!()
    }

    #[test]
    fn test_query_p() {
        todo!()
    }

    #[test]
    fn test_query_po() {
        todo!()
    }

    #[test]
    fn test_query_o() {
        todo!()
    }
}
