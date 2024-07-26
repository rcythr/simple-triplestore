use serde::{de::DeserializeOwned, Serialize};

use crate::{Mergeable, TripleStoreMerge};

use super::SledTripleStore;

impl<
        NodeProperties: Clone + Mergeable + Serialize + DeserializeOwned,
        EdgeProperties: Clone + Mergeable + Serialize + DeserializeOwned,
    > TripleStoreMerge<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn merge(&mut self, other: Self) {
        todo!()
    }

    fn merge_node(&mut self, node: ulid::Ulid, data: NodeProperties) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (ulid::Ulid, NodeProperties)>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_edge(
        &mut self,
        triple: crate::Triple,
        data: EdgeProperties,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (crate::Triple, EdgeProperties)>,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}
