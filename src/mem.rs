use crate::{
    DecoratedTriple, Mergeable, Triple, TripleStoreError, TripleStoreIntoIter, TripleStoreMerge,
};
use std::collections::{BTreeMap, HashMap};
use ulid::Ulid;

mod extend;
mod insert;
mod iter;
mod merge;
mod query;
mod remove;

/// A triple store implemented entirely in memory.
pub struct MemTripleStore<NodeProperties, EdgeProperties> {
    node_props: HashMap<Ulid, NodeProperties>,
    edge_props: HashMap<Ulid, EdgeProperties>,
    spo_data: BTreeMap<[u8; 48], Ulid>,
    pos_data: BTreeMap<[u8; 48], Ulid>,
    osp_data: BTreeMap<[u8; 48], Ulid>,
}

impl<NodeProperties: Clone, EdgeProperties: Clone> MemTripleStore<NodeProperties, EdgeProperties> {
    pub fn new() -> Self {
        Self {
            node_props: HashMap::new(),
            edge_props: HashMap::new(),
            spo_data: BTreeMap::new(),
            pos_data: BTreeMap::new(),
            osp_data: BTreeMap::new(),
        }
    }
}

impl<NodeProperties: Clone, EdgeProperties: Clone> TripleStoreError
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type Error = ();
}
