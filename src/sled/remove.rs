use serde::{de::DeserializeOwned, Serialize};

use crate::TripleStoreRemove;

use super::Error;
use super::SledTripleStore;

impl<
        NodeProperties: Clone + Serialize + DeserializeOwned,
        EdgeProperties: Clone + Serialize + DeserializeOwned,
    > TripleStoreRemove<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn remove_node(&mut self, node: &ulid::Ulid) -> Result<(), Error> {
        todo!()
    }

    fn remove_node_batch(
        &mut self,
        nodes: impl Iterator<Item = ulid::Ulid>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_edge(&mut self, triple: crate::Triple) -> Result<(), Error> {
        todo!()
    }

    fn remove_edge_batch(
        &mut self,
        triples: impl Iterator<Item = crate::Triple>,
    ) -> Result<(), Error> {
        todo!()
    }
}
