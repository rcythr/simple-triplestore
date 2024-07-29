use serde::{de::DeserializeOwned, Serialize};

use crate::{prelude::*, PropertiesType};

impl<
        NodeProperties: PropertiesType + Mergeable + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Mergeable + Serialize + DeserializeOwned,
    > TripleStoreMerge<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn merge<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>> {
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

#[cfg(test)]
mod test {
    #[test]
    fn test_merge() {
        todo!()
    }

    #[test]
    fn test_merge_node() {
        todo!()
    }

    #[test]
    fn test_merge_node_batch() {
        todo!()
    }

    #[test]
    fn test_merge_edge() {
        todo!()
    }

    #[test]
    fn test_merge_edge_batch() {
        todo!()
    }
}
