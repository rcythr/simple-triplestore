use serde::{de::DeserializeOwned, Serialize};

use crate::{prelude::*, traits::IdType, Property};

impl<
        Id: IdType,
        NodeProps: Property + Mergeable + Serialize + DeserializeOwned,
        EdgeProps: Property + Mergeable + Serialize + DeserializeOwned,
    > TripleStoreMerge<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn merge<E: std::fmt::Debug>(
        &mut self,
        _other: impl TripleStore<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>> {
        todo!()
    }

    fn merge_node(&mut self, _node: Id, _data: NodeProps) -> Result<(), Self::Error> {
        todo!()
    }

    fn merge_edge(&mut self, _triple: Triple<Id>, _data: EdgeProps) -> Result<(), Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    // #[test]
    // fn test_merge() {
    //     let mut temp_dirs = Vec::new();
    //     crate::conformance::merge::test_merge(|| {
    //         let (temp_dir, db) = crate::sled::create_test_db().expect("ok");
    //         let sled_db = SledTripleStore::new(&db).expect("ok");
    //         temp_dirs.push((temp_dir, db));
    //         sled_db
    //     });
    // }

    //     #[test]
    //     fn test_merge_node() {
    //         let mut temp_dirs = Vec::new();
    //         crate::conformance::merge::test_merge_node(|| {
    //             let (temp_dir, db) = crate::sled::create_test_db().expect("ok");
    //             let sled_db = SledTripleStore::new(&db).expect("ok");
    //             temp_dirs.push((temp_dir, db));
    //             sled_db
    //         });
    //     }

    //     #[test]
    //     fn test_merge_edge() {
    //         let mut temp_dirs = Vec::new();
    //         crate::conformance::merge::test_merge_edge(|| {
    //             let (temp_dir, db) = crate::sled::create_test_db().expect("ok");
    //             let sled_db = SledTripleStore::new(&db).expect("ok");
    //             temp_dirs.push((temp_dir, db));
    //             sled_db
    //         });
    //     }
}
