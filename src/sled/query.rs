use serde::{de::DeserializeOwned, Serialize};

use crate::{MemTripleStore, PropertyType, TripleStoreQuery};

use super::SledTripleStore;

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreQuery<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    fn run(&self, query: crate::Query) -> Result<Self::QueryResult, Self::Error> {
        match query {
            crate::Query::NodeProps(_) => todo!(),
            crate::Query::SPO(_) => todo!(),
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
    use crate::prelude::*;

    // #[test]
    // fn test_query_node_props() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_node_props(sled_db);
    // }

    // #[test]
    // fn test_query_edge_props() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_edge_props(sled_db);
    // }

    // #[test]
    // fn test_query_s() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_s(sled_db);
    // }

    // #[test]
    // fn test_query_sp() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_sp(sled_db);
    // }

    // #[test]
    // fn test_query_p() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_p(sled_db);
    // }

    // #[test]
    // fn test_query_po() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_po(sled_db);
    // }

    // #[test]
    // fn test_query_o() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_o(sled_db);
    // }

    // #[test]
    // fn test_query_os() {
    //     let (_tempdir, db) = crate::sled::create_test_db().expect("ok");
    //     let sled_db = SledTripleStore::new(&db).expect("ok");
    //     crate::conformance::query::test_query_os(sled_db);
    // }
}
