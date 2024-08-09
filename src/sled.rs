use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
};

use crate::{
    prelude::*,
    traits::{ConcreteIdType, Property},
    IdGenerator,
};
use serde::{de::DeserializeOwned, Serialize};

mod extend;
mod insert;
mod iter;
mod merge;
mod query;
mod remove;

#[derive(Debug)]
pub enum SledTripleStoreError {
    SledError(sled::Error),
    SerializationError(bincode::Error),
    KeySizeError,
    MissingPropertyData,
}

impl From<sled::Error> for SledTripleStoreError {
    fn from(e: sled::Error) -> Self {
        SledTripleStoreError::SledError(e)
    }
}

impl From<bincode::Error> for SledTripleStoreError {
    fn from(e: bincode::Error) -> Self {
        SledTripleStoreError::SerializationError(e)
    }
}

/// A triplestore which is backed by [sled](https://sled.rs).
///
/// # Example
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::{prelude::*, SledTripleStore, PropsTriple, Triple, UlidIdGenerator, EdgeOrder};
/// let temp_dir = tempdir::TempDir::new("sled").unwrap();
/// let sled_db = sled::open(temp_dir.path()).unwrap();
///
/// let mut db = SledTripleStore::new(&sled_db, UlidIdGenerator::new())?;
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
/// # Ok::<(), simple_triplestore::sled::SledTripleStoreError>(())
/// ```
///
/// We can do arbitrary queries, e.g.:
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::{prelude::*, SledTripleStore, PropsTriple, Triple, UlidIdGenerator, EdgeOrder, QueryError};
/// # let temp_dir = tempdir::TempDir::new("sled").unwrap();
/// # let sled_db = sled::open(temp_dir.path()).unwrap();
/// # let mut db = SledTripleStore::new(&sled_db, UlidIdGenerator::new()).unwrap();
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
/// # Ok::<(), QueryError<simple_triplestore::sled::SledTripleStoreError, ()>>(())
/// ```
pub struct SledTripleStore<
    Id: ConcreteIdType,
    NodeProps: Property + Serialize + DeserializeOwned,
    EdgeProps: Serialize + DeserializeOwned,
> {
    _phantom: std::marker::PhantomData<(Id, NodeProps, EdgeProps)>,
    node_props: sled::Tree,
    edge_props: sled::Tree,
    spo_data: sled::Tree,
    pos_data: sled::Tree,
    osp_data: sled::Tree,
    id_generator: Box<dyn IdGenerator<Id> + Send + Sync>,
}

impl<
        Id: ConcreteIdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > SledTripleStore<Id, NodeProps, EdgeProps>
{
    pub fn new(
        db: &sled::Db,
        id_generator: impl IdGenerator<Id> + 'static,
    ) -> Result<Self, SledTripleStoreError> {
        let node_data = db
            .open_tree(b"node_data")
            .map_err(|e| SledTripleStoreError::SledError(e))?;
        let edge_data = db.open_tree(b"edge_data")?;
        let spo_data = db.open_tree(b"spo_data")?;
        let pos_data = db.open_tree(b"pos_data")?;
        let osp_data = db.open_tree(b"osp_data")?;

        Ok(Self {
            node_props: node_data,
            edge_props: edge_data,
            spo_data,
            pos_data,
            osp_data,
            id_generator: Box::new(id_generator),
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<
        Id: ConcreteIdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStoreError for SledTripleStore<Id, NodeProps, EdgeProps>
{
    type Error = SledTripleStoreError;
}

impl<
        Id: ConcreteIdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > TripleStore<Id, NodeProps, EdgeProps> for SledTripleStore<Id, NodeProps, EdgeProps>
{
}

impl<
        Id: ConcreteIdType,
        NodeProps: Property + Serialize + DeserializeOwned,
        EdgeProps: Property + Serialize + DeserializeOwned,
    > std::fmt::Debug for SledTripleStore<Id, NodeProps, EdgeProps>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SledTripleStore:\n")?;
        f.write_str(" Node Properties:\n")?;
        for r in self.node_props.iter() {
            let (id, node_props) = r.map_err(|_| std::fmt::Error)?;
            f.write_fmt(format_args!(
                "  {} -> {:?}\n",
                Id::try_from_be_bytes(&id).ok_or(std::fmt::Error)?,
                bincode::deserialize(&node_props).map_err(|_| std::fmt::Error)?
            ))?;
        }

        // When printing edge properties, we display the edge hash instead of the Ulid because it
        // will be stable across graphs whereas the Ulid is not stable.
        //
        // Any of the edge hashes would work here, but spo is chosen arbitrarily.
        f.write_str(" Edge Properties:\n")?;

        // Construct: [Ulid] -> [u64] (SPO Edge hash)
        let ulid_to_spo_edge_hash = self
            .spo_data
            .iter()
            .map(|r| r.map_err(|_| std::fmt::Error))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(k, v)| {
                let hash;
                {
                    let mut hash_builder = std::hash::DefaultHasher::new();
                    k.as_ref().hash(&mut hash_builder);
                    hash = hash_builder.finish();
                }
                (v.clone(), hash)
            })
            .collect::<HashMap<_, _>>();

        // Use [Ulid] -> u64 on the keys of edge_props: [Ulid -> & Edge Properties] to produce:
        //
        //  [u64] -> [& Edge Properties]
        //
        // By using BTreeMap here, we get a nice print order.
        let hash_to_edge_data = self
            .edge_props
            .iter()
            .map(|r| r.map_err(|_| std::fmt::Error))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|(ulid, edge_data)| match ulid_to_spo_edge_hash.get(&ulid) {
                Some(hash) => (Some(hash), edge_data),
                None => (None, edge_data),
            })
            .collect::<BTreeMap<_, _>>();

        for (hash, node_props) in hash_to_edge_data {
            match hash {
                None => {
                    f.write_fmt(format_args!("  _ -> {:?}\n", node_props))?;
                }
                Some(hash) => {
                    f.write_fmt(format_args!("  {:#016x} -> {:?}\n", hash, node_props))?;
                }
            }
        }

        f.write_str(" Edges (SPO):\n")?;
        for r in self.spo_data.iter() {
            let (triple, ulid) = r.map_err(|_| std::fmt::Error)?;

            let triple =
                Id::decode_spo_triple(&triple[..].try_into().map_err(|_| std::fmt::Error)?);
            f.write_fmt(format_args!(
                "  ({}, {}, {}) -> ",
                triple.sub, triple.pred, triple.obj
            ))?;
            match ulid_to_spo_edge_hash.get(&ulid) {
                Some(hash) => {
                    f.write_fmt(format_args!("{:#016x}\n", hash))?;
                }
                None => {
                    f.write_str("_\n")?;
                }
            }
        }

        f.write_str(" Edges (POS):\n")?;
        for r in self.pos_data.iter() {
            let (triple, ulid) = r.map_err(|_| std::fmt::Error)?;

            let triple =
                Id::decode_pos_triple(&triple[..].try_into().map_err(|_| std::fmt::Error)?);
            f.write_fmt(format_args!(
                "  ({}, {}, {}) -> ",
                triple.sub, triple.pred, triple.obj
            ))?;
            match ulid_to_spo_edge_hash.get(&ulid) {
                Some(hash) => {
                    f.write_fmt(format_args!("{:#016x}\n", hash))?;
                }
                None => {
                    f.write_str("_\n")?;
                }
            }
        }

        f.write_str(" Edges (OSP):\n")?;
        for r in self.osp_data.iter() {
            let (triple, ulid) = r.map_err(|_| std::fmt::Error)?;

            let triple =
                Id::decode_osp_triple(&triple[..].try_into().map_err(|_| std::fmt::Error)?);
            f.write_fmt(format_args!(
                "  ({}, {}, {}) -> ",
                triple.sub, triple.pred, triple.obj
            ))?;
            match ulid_to_spo_edge_hash.get(&ulid) {
                Some(hash) => {
                    f.write_fmt(format_args!("{:#016x}\n", hash))?;
                }
                None => {
                    f.write_str("_\n")?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) fn create_test_db() -> Result<(tempdir::TempDir, sled::Db), sled::Error> {
    let temp_dir = tempdir::TempDir::new("SledTripleStore")?;
    let db = sled::open(temp_dir.path())?;
    Ok((temp_dir, db))
}

#[cfg(feature = "rdf")]
mod rdf {
    use serde::{de::DeserializeOwned, Serialize};
    use sled::transaction::{ConflictableTransactionError, Transactional};

    use crate::traits::{BidirIndex, IndexType};

    #[derive(Debug)]
    pub enum SledHashIndexError<Left, Right> {
        DuplicateRight(Left, Right, Right),
        DuplicateLeft(Right, Left, Left),
        SledError(sled::Error),
        SerializationError(bincode::Error),
    }

    pub struct SledHashIndex<
        Left: IndexType + Serialize + DeserializeOwned,
        Right: IndexType + Serialize + DeserializeOwned,
    > {
        left_to_right: sled::Tree,
        right_to_left: sled::Tree,
        _phantom: std::marker::PhantomData<(Left, Right)>,
    }

    impl<
            Left: IndexType + Serialize + DeserializeOwned,
            Right: IndexType + Serialize + DeserializeOwned,
        > SledHashIndex<Left, Right>
    {
        pub fn new(db: &sled::Db) -> Result<Self, sled::Error> {
            Ok(Self {
                left_to_right: db.open_tree("left_to_right")?,
                right_to_left: db.open_tree("right_to_left")?,
                _phantom: std::marker::PhantomData,
            })
        }
    }

    impl<
            Left: IndexType + Serialize + DeserializeOwned,
            Right: IndexType + Serialize + DeserializeOwned,
        > BidirIndex for SledHashIndex<Left, Right>
    {
        type Left = Left;
        type Right = Right;
        type Error = SledHashIndexError<Left, Right>;

        fn set(&mut self, left: Self::Left, right: Self::Right) -> Result<(), Self::Error> {
            let left_bytes =
                bincode::serialize(&left).map_err(|e| SledHashIndexError::SerializationError(e))?;
            let right_bytes = bincode::serialize(&right)
                .map_err(|e| SledHashIndexError::SerializationError(e))?;

            (&self.left_to_right, &self.right_to_left)
                .transaction(|(left_to_right, right_to_left)| {
                    match left_to_right.get(left_bytes.as_slice())? {
                        None => {
                            left_to_right.insert(left_bytes.as_slice(), right_bytes.as_slice())?;
                            Ok(())
                        }
                        Some(existing_right) => {
                            let existing_right =
                                bincode::deserialize(&existing_right).map_err(|e| {
                                    ConflictableTransactionError::Abort(
                                        SledHashIndexError::SerializationError(e),
                                    )
                                })?;

                            Err(ConflictableTransactionError::Abort(
                                SledHashIndexError::DuplicateRight(
                                    left.clone(),
                                    existing_right,
                                    right.clone(),
                                ),
                            ))
                        }
                    }?;

                    match right_to_left.get(right_bytes.as_slice())? {
                        None => {
                            right_to_left.insert(right_bytes.as_slice(), left_bytes.as_slice())?;
                            Ok(())
                        }
                        Some(existing_left) => {
                            let existing_left =
                                bincode::deserialize(&existing_left).map_err(|e| {
                                    ConflictableTransactionError::Abort(
                                        SledHashIndexError::SerializationError(e),
                                    )
                                })?;

                            Err(ConflictableTransactionError::Abort(
                                SledHashIndexError::DuplicateLeft(
                                    right.clone(),
                                    existing_left,
                                    left.clone(),
                                ),
                            ))
                        }
                    }?;

                    Ok(())
                })
                .map_err(|e| match e {
                    sled::transaction::TransactionError::Abort(e) => e,
                    sled::transaction::TransactionError::Storage(e) => {
                        SledHashIndexError::SledError(e)
                    }
                })
        }

        fn left_to_right(&self, left: &Self::Left) -> Result<Option<Self::Right>, Self::Error> {
            let left =
                bincode::serialize(&left).map_err(|e| SledHashIndexError::SerializationError(e))?;

            self.left_to_right
                .get(&left)
                .map_err(|e| SledHashIndexError::SledError(e))?
                .map(|right| {
                    bincode::deserialize(right.as_ref())
                        .map_err(|e| SledHashIndexError::SerializationError(e))
                })
                .transpose()
        }

        fn right_to_left(&self, right: &Self::Right) -> Result<Option<Self::Left>, Self::Error> {
            let right = bincode::serialize(&right)
                .map_err(|e| SledHashIndexError::SerializationError(e))?;

            self.right_to_left
                .get(&right)
                .map_err(|e| SledHashIndexError::SledError(e))?
                .map(|left| {
                    bincode::deserialize(left.as_ref())
                        .map_err(|e| SledHashIndexError::SerializationError(e))
                })
                .transpose()
        }
    }
}

#[cfg(feature = "rdf")]
pub use rdf::*;
