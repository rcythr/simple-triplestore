use crate::{prelude::*, PropertyType};
use serde::{de::DeserializeOwned, Serialize};
use sled::IVec;
use ulid::Ulid;

mod extend;
mod insert;
mod iter;
mod merge;
mod query;
mod remove;

#[derive(Debug)]
pub enum Error {
    SledError(sled::Error),
    SerializationError(bincode::Error),
    KeySizeError,
    MissingPropertyData,
}

/// A triplestore which is backed by [sled](https://sled.rs).
///
/// # Example
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// let temp_dir = tempdir::TempDir::new("sled").unwrap();
/// let sled_db = sled::open(temp_dir.path()).unwrap();
///
/// let mut db = SledTripleStore::new(&sled_db)?;
///
/// // Get some identifiers. These will probably come from an index such as `Readable Name -> Ulid`
/// let node_1 = Ulid(123);
/// let node_2 = Ulid(456);
/// let node_3 = Ulid(789);
/// let edge = Ulid(999);
///
/// // We can insert nodes and edges with user-defined property types.
/// // For a given TripleStore we can have one type for Nodes and one for Edges.
/// db.insert_node(node_1, "foo".to_string())?;
/// db.insert_node(node_2, "bar".to_string())?;
/// db.insert_node(node_3, "baz".to_string())?;
/// db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_2}, Vec::from([1,2,3]))?;
/// db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6]))?;
///
/// // Three vertices with correct properties.
/// assert_eq!(
///   db.iter_vertices()
///     .map(|r| r.expect("ok"))
///     .collect::<Vec<_>>(),  
///   [
///     (node_1, "foo".to_string()),
///     (node_2, "bar".to_string()),
///     (node_3, "baz".to_string())
///   ]
/// );
///
/// // Two edges with the correct properties.
/// assert_eq!(
///   db.iter_edges_with_props(EdgeOrder::default())
///     .map(|r| r.expect("ok"))
///     .collect::<Vec<_>>(),
///   [
///     PropsTriple{
///       sub: (node_1, "foo".to_string()),
///       pred: (edge, Vec::from([1,2,3])),
///       obj: (node_2, "bar".to_string())},
///     PropsTriple{
///       sub: (node_1, "foo".to_string()),
///       pred: (edge, Vec::from([4,5,6])),
///       obj: (node_3, "baz".to_string())}
///   ]
/// );
/// # Ok::<(), simple_triplestore::sled::Error>(())
/// ```
///
/// We can do arbitrary queries, e.g.:
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let temp_dir = tempdir::TempDir::new("sled").unwrap();
/// # let sled_db = sled::open(temp_dir.path()).unwrap();
/// # let mut db = SledTripleStore::new(&sled_db).unwrap();
/// # let node_1 = Ulid(123);
/// # let node_2 = Ulid(456);
/// # let node_3 = Ulid(789);
/// # let edge = Ulid(999);
/// # db.insert_node(node_1, "foo".to_string()).unwrap();
/// # db.insert_node(node_2, "bar".to_string()).unwrap();
/// # db.insert_node(node_3, "baz".to_string()).unwrap();
/// # db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_2}, Vec::from([1,2,3])).unwrap();
/// # db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6])).unwrap();
/// // 1. Edges where node_3 is the object.
/// assert_eq!(
///   db.run(query!{ ? -?-> [node_3] })?
///     .iter_edges(EdgeOrder::default())
///     .map(|r| r.expect("ok"))
///     .collect::<Vec<_>>(),
///   [
///     (Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6])),
///   ]
/// );
///
/// // Edges with `edge` as the predicate.
/// assert_eq!(
///   db.run(query!{ ? -[edge]-> ? })?
///     .iter_edges(EdgeOrder::default())
///     .map(|r| r.expect("ok"))
///     .collect::<Vec<_>>(),
///   [
///     (Triple{sub: node_1, pred: edge, obj: node_2}, Vec::from([1,2,3])),
///     (Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6])),
///   ]
/// );
///
/// # Ok::<(), QueryError<simple_triplestore::sled::Error, ()>>(())
/// ```
pub struct SledTripleStore<
    NodeProperties: PropertyType + Serialize + DeserializeOwned,
    EdgeProperties: Serialize + DeserializeOwned,
> {
    _phantom: std::marker::PhantomData<(NodeProperties, EdgeProperties)>,
    node_props: sled::Tree,
    edge_props: sled::Tree,
    spo_data: sled::Tree,
    pos_data: sled::Tree,
    osp_data: sled::Tree,
}

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > SledTripleStore<NodeProperties, EdgeProperties>
{
    pub fn new(db: &sled::Db) -> Result<Self, Error> {
        let node_data = db
            .open_tree(b"node_data")
            .map_err(|e| Error::SledError(e))?;
        let edge_data = db
            .open_tree(b"edge_data")
            .map_err(|e| Error::SledError(e))?;
        let spo_data = db.open_tree(b"spo_data").map_err(|e| Error::SledError(e))?;
        let pos_data = db.open_tree(b"pos_data").map_err(|e| Error::SledError(e))?;
        let osp_data = db.open_tree(b"osp_data").map_err(|e| Error::SledError(e))?;

        Ok(Self {
            node_props: node_data,
            edge_props: edge_data,
            spo_data,
            pos_data,
            osp_data,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreError for SledTripleStore<NodeProperties, EdgeProperties>
{
    type Error = Error;
}

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStore<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
}

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > std::fmt::Debug for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn create_test_db() -> Result<(tempdir::TempDir, sled::Db), sled::Error> {
    let temp_dir = tempdir::TempDir::new("SledTripleStore")?;
    let db = sled::open(temp_dir.path())?;
    Ok((temp_dir, db))
}

