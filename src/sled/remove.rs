use std::borrow::Borrow;

use serde::{de::DeserializeOwned, Serialize};
use ulid::Ulid;

use crate::PropertyType;
use crate::TripleStoreRemove;

use super::Error;
use super::SledTripleStore;

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreRemove<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn remove_node(&mut self, _node: impl Borrow<Ulid>) -> Result<(), Error> {
        todo!()
    }

    fn remove_node_batch<I: IntoIterator<Item = impl Borrow<ulid::Ulid>>>(
        &mut self,
        _nodes: I,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn remove_edge(&mut self, _triple: crate::Triple) -> Result<(), Error> {
        todo!()
    }

    fn remove_edge_batch<I: IntoIterator<Item = crate::Triple>>(
        &mut self,
        _triples: I,
    ) -> Result<(), Error> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    // #[test]
    // fn test_remove_node() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::remove::test_remove_node(sled_db);
    // }

    // #[test]
    // fn test_remove_node_batch() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::remove::test_remove_node_batch(sled_db);
    // }

    // #[test]
    // fn test_remove_edge() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::remove::test_remove_edge(sled_db);
    // }

    // #[test]
    // fn test_remove_edge_batch() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::remove::test_remove_edge_batch(sled_db);
    // }
}
