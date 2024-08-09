use crate::{
    prelude::*,
    traits::{ConcreteIdType, Property},
    IdGenerator,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{Hash, Hasher},
};

mod extend;
mod insert;
mod iter;
mod merge;
mod query;
mod remove;
mod set;

/// A triple store implemented entirely in memory using [BTreeMap][std::collections::BTreeMap].
///
/// # Example
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::{prelude::*, MemTripleStore, PropsTriple, Triple, UlidIdGenerator, EdgeOrder};
/// let mut db = MemTripleStore::new(UlidIdGenerator::new());
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
/// # Ok::<(), ()>(())
/// ```
///
/// We can do arbitrary queries, e.g.:
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::{prelude::*, MemTripleStore, PropsTriple, Triple, UlidIdGenerator, EdgeOrder, QueryError};
/// # let mut db = MemTripleStore::new(UlidIdGenerator::new());
/// # let node_1 = Ulid(123);
/// # let node_2 = Ulid(456);
/// # let node_3 = Ulid(789);
/// # let edge = Ulid(999);
/// # db.insert_node(node_1, "foo".to_string()).expect("ok");
/// # db.insert_node(node_2, "bar".to_string()).expect("ok");
/// # db.insert_node(node_3, "baz".to_string()).expect("ok");
/// # db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_2}, Vec::from([1,2,3])).expect("ok");
/// # db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6])).expect("ok");
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
/// # Ok::<(), QueryError<(), ()>>(())
/// ```
pub struct MemTripleStore<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property> {
    node_props: BTreeMap<Id, NodeProps>,
    edge_props: BTreeMap<Id, EdgeProps>,
    spo_data: BTreeMap<Id::TripleByteArrayType, Id>,
    pos_data: BTreeMap<Id::TripleByteArrayType, Id>,
    osp_data: BTreeMap<Id::TripleByteArrayType, Id>,
    id_generator: Box<dyn IdGenerator<Id>>,
}

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property> std::fmt::Debug
    for MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MemTripleStore:\n")?;
        f.write_str(" Node Properties:\n")?;
        for (id, node_props) in self.node_props.iter() {
            f.write_fmt(format_args!("  {} -> {:?}\n", id, node_props))?;
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
            .map(|(k, v)| {
                let hash;
                {
                    let mut hash_builder = std::hash::DefaultHasher::new();
                    k.hash(&mut hash_builder);
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
            .map(|(ulid, edge_data)| match ulid_to_spo_edge_hash.get(ulid) {
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
        for (triple, ulid) in self.spo_data.iter() {
            let triple = Id::decode_spo_triple(&triple);
            f.write_fmt(format_args!(
                "  ({}, {}, {}) -> ",
                triple.sub, triple.pred, triple.obj
            ))?;
            match ulid_to_spo_edge_hash.get(ulid) {
                Some(hash) => {
                    f.write_fmt(format_args!("{:#016x}\n", hash))?;
                }
                None => {
                    f.write_str("_\n")?;
                }
            }
        }

        f.write_str(" Edges (POS):\n")?;
        for (triple, ulid) in self.pos_data.iter() {
            let triple = Id::decode_pos_triple(&triple);
            f.write_fmt(format_args!(
                "  ({}, {}, {}) -> ",
                triple.sub, triple.pred, triple.obj
            ))?;
            match ulid_to_spo_edge_hash.get(ulid) {
                Some(hash) => {
                    f.write_fmt(format_args!("{:#016x}\n", hash))?;
                }
                None => {
                    f.write_str("_\n")?;
                }
            }
        }

        f.write_str(" Edges (OSP):\n")?;
        for (triple, ulid) in self.osp_data.iter() {
            let triple = Id::decode_osp_triple(&triple);
            f.write_fmt(format_args!(
                "  ({}, {}, {}) -> ",
                triple.sub, triple.pred, triple.obj
            ))?;
            match ulid_to_spo_edge_hash.get(ulid) {
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

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property> PartialEq
    for MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn eq(&self, other: &Self) -> bool {
        if !self.node_props.eq(&other.node_props) {
            return false;
        }

        // We expect edge data to be identical, so zip them together and test that they match.
        let mut cached_comparisons: HashSet<(Id, Id)> = HashSet::new();
        let mut eq_edge_prop_by_id = |self_edge_prop_id, other_edge_prop_id| {
            let self_edge_props = self.edge_props.get(&self_edge_prop_id);
            let other_edge_props = other.edge_props.get(&other_edge_prop_id);

            // If either side is missing, we say that the overall result is false.
            if self_edge_props.is_none() || other_edge_props.is_none() {
                return false;
            }

            // If we've seen this before (perhaps on a different ordering), it's true.
            if cached_comparisons.contains(&(self_edge_prop_id, other_edge_prop_id)) {
                return true;
            }

            // Test that the edge properties match.
            if self_edge_props == other_edge_props {
                cached_comparisons.insert((self_edge_prop_id, other_edge_prop_id));
                true
            } else {
                false
            }
        };

        let mut check_edge =
            move |((self_edge, self_edge_prop_id), (other_edge, other_edge_prop_id)): (
                (&Id::TripleByteArrayType, &Id),
                (&Id::TripleByteArrayType, &Id),
            )| {
                // Test the Keys
                if self_edge.ne(other_edge) {
                    return false;
                }
                // Test the Values
                eq_edge_prop_by_id(self_edge_prop_id.clone(), other_edge_prop_id.clone())
            };

        // SPO
        for edge_pair in self.spo_data.iter().zip(other.spo_data.iter()) {
            if !check_edge(edge_pair) {
                return false;
            }
        }

        true
    }
}

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property>
    MemTripleStore<Id, NodeProps, EdgeProps>
{
    pub fn new(id_generator: impl IdGenerator<Id> + 'static) -> Self {
        Self::new_from_boxed_id_generator(Box::new(id_generator))
    }

    pub(crate) fn new_from_boxed_id_generator(
        id_generator: Box<dyn IdGenerator<Id> + 'static>,
    ) -> Self {
        Self {
            node_props: BTreeMap::new(),
            edge_props: BTreeMap::new(),
            spo_data: BTreeMap::new(),
            pos_data: BTreeMap::new(),
            osp_data: BTreeMap::new(),
            id_generator: id_generator,
        }
    }
}

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property>
    TripleStore<Id, NodeProps, EdgeProps> for MemTripleStore<Id, NodeProps, EdgeProps>
{
}

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property> TripleStoreError
    for MemTripleStore<Id, NodeProps, EdgeProps>
{
    type Error = ();
}

#[cfg(feature = "rdf")]
mod rdf {
    use std::collections::HashMap;

    use crate::traits::{BidirIndex, IndexType};

    #[derive(Debug)]
    pub enum MemHashIndexError<Left, Right> {
        DuplicateRight(Left, Right, Right),
        DuplicateLeft(Right, Left, Left),
    }

    pub struct MemHashIndex<Left: IndexType, Right: IndexType> {
        left_to_right: HashMap<Left, Right>,
        right_to_left: HashMap<Right, Left>,
    }

    impl<Left: IndexType, Right: IndexType> MemHashIndex<Left, Right> {
        pub fn new() -> Self {
            Self {
                left_to_right: HashMap::new(),
                right_to_left: HashMap::new(),
            }
        }
    }

    impl<Left: IndexType, Right: IndexType> BidirIndex for MemHashIndex<Left, Right> {
        type Left = Left;
        type Right = Right;
        type Error = MemHashIndexError<Left, Right>;

        fn set(&mut self, left: Self::Left, right: Self::Right) -> Result<(), Self::Error> {
            match self.left_to_right.entry(left.clone()) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(right.clone());
                    Ok(())
                }
                std::collections::hash_map::Entry::Occupied(o) => {
                    if right != *o.get() {
                        Err(MemHashIndexError::DuplicateRight(
                            o.key().clone(),
                            right.clone(),
                            o.get().clone(),
                        ))
                    } else {
                        Ok(())
                    }
                }
            }?;

            match self.right_to_left.entry(right.clone()) {
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(left.clone());
                    Ok(())
                }
                std::collections::hash_map::Entry::Occupied(o) => {
                    if left != *o.get() {
                        Err(MemHashIndexError::DuplicateLeft(
                            o.key().clone(),
                            left.clone(),
                            o.get().clone(),
                        ))
                    } else {
                        Ok(())
                    }
                }
            }?;

            Ok(())
        }

        fn left_to_right(&self, left: &Self::Left) -> Result<Option<Self::Right>, Self::Error> {
            Ok(self.left_to_right.get(left).cloned())
        }

        fn right_to_left(&self, right: &Self::Right) -> Result<Option<Self::Left>, Self::Error> {
            Ok(self.right_to_left.get(right).cloned())
        }
    }
}

#[cfg(feature = "rdf")]
pub use rdf::*;
