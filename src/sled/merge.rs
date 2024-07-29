use serde::{de::DeserializeOwned, Serialize};

use crate::{prelude::*, PropertyType};

impl<
        NodeProperties: PropertyType + Mergeable + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Mergeable + Serialize + DeserializeOwned,
    > TripleStoreMerge<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn merge<E: std::fmt::Debug>(
        &mut self,
        _other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>> {
        todo!()
    }

    fn merge_node(&mut self, _node: ulid::Ulid, _data: NodeProperties) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_node_batch<I: IntoIterator<Item = (ulid::Ulid, NodeProperties)>>(
        &mut self,
        _nodes: I,
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

    fn merge_edge_batch<I: IntoIterator<Item = (crate::Triple, EdgeProperties)>>(
        &mut self,
        _triples: I,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}

// #[cfg(test)]
// mod test {
//     #[test]
//     fn test_merge() {
//         todo!()
//     }

//     #[test]
//     fn test_merge_node() {
//         todo!()
//     }

//     #[test]
//     fn test_merge_node_batch() {
//         todo!()
//     }

//     #[test]
//     fn test_merge_edge() {
//         todo!()
//     }

//     #[test]
//     fn test_merge_edge_batch() {
//         todo!()
//     }
// }
