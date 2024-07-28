use serde::{de::DeserializeOwned, Serialize};

use crate::{Mergeable, PropertiesType, TripleStoreMerge};

use super::SledTripleStore;

impl<
        NodeProperties: PropertiesType + Mergeable + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Mergeable + Serialize + DeserializeOwned,
    > TripleStoreMerge<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn merge(&mut self, _other: Self) {
        todo!()
    }

    fn merge_node(&mut self, _node: ulid::Ulid, _data: NodeProperties) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_node_batch(
        &mut self,
        _nodes: impl Iterator<Item = (ulid::Ulid, NodeProperties)>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_edge(
        &mut self,
        _triple: crate::Triple,
        _data: EdgeProperties,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_edge_batch(
        &mut self,
        _triples: impl Iterator<Item = (crate::Triple, EdgeProperties)>,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}
