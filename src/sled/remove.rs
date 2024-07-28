use serde::{de::DeserializeOwned, Serialize};

use crate::PropertiesType;
use crate::TripleStoreRemove;

use super::Error;
use super::SledTripleStore;

impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned,
    > TripleStoreRemove<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn remove_node(&mut self, _node: &ulid::Ulid) -> Result<(), Error> {
        todo!()
    }

    fn remove_node_batch(
        &mut self,
        _nodes: impl Iterator<Item = ulid::Ulid>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_edge(&mut self, _triple: crate::Triple) -> Result<(), Error> {
        todo!()
    }

    fn remove_edge_batch(
        &mut self,
        _triples: impl Iterator<Item = crate::Triple>,
    ) -> Result<(), Error> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_remove_node() {
        todo!()
    }

    #[test]
    fn test_remove_node_batch() {
        todo!()
    }

    #[test]
    fn test_remove_edge() {
        todo!()
    }

    #[test]
    fn test_remove_edge_batch() {
        todo!()
    }
}
