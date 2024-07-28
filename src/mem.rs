use crate::{PropertiesType, Triple, TripleStoreError};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{Hash, Hasher},
};
use ulid::Ulid;

mod extend;
mod insert;
mod iter;
mod merge;
mod query;
mod remove;
mod set;

/// A triple store implemented entirely in memory using [std::collections::BTreeMap].
#[derive(Clone)]
pub struct MemTripleStore<NodeProperties: PropertiesType, EdgeProperties: PropertiesType> {
    node_props: BTreeMap<Ulid, NodeProperties>,
    edge_props: BTreeMap<Ulid, EdgeProperties>,
    spo_data: BTreeMap<[u8; 48], Ulid>,
    pos_data: BTreeMap<[u8; 48], Ulid>,
    osp_data: BTreeMap<[u8; 48], Ulid>,
}

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType> std::fmt::Debug
    for MemTripleStore<NodeProperties, EdgeProperties>
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

        f.write_str(" Edge Properties:\n")?;
        f.write_str(" Edges (SPO):\n")?;
        for (triple, ulid) in self.spo_data.iter() {
            let triple = Triple::decode_spo(&triple);
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
            let triple = Triple::decode_pos(&triple);
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
            let triple = Triple::decode_osp(&triple);
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

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType> PartialEq
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn eq(&self, other: &Self) -> bool {
        if !self.node_props.eq(&other.node_props) {
            return false;
        }

        // We expect edge data to be identical, so zip them together and test that they match.
        let mut cached_comparisons: HashSet<(Ulid, Ulid)> = HashSet::new();
        let mut eq_edge_prop_by_id = |self_edge_prop_id, other_edge_prop_id| {
            let self_edge_props = self.edge_props.get(&self_edge_prop_id);
            let other_edge_props = other.edge_props.get(&other_edge_prop_id);

            // If either side is missing, we say that the overall result is false.
            if self_edge_props.is_none() || other_edge_props.is_none() {
                return false;
            }

            // If we've seen this before (perhaps on a different ordering), it's true.
            if cached_comparisons.contains(&(self_edge_prop_id, self_edge_prop_id)) {
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

        let mut check_edge = move |(
            (self_edge, self_edge_prop_id),
            (other_edge, other_edge_prop_id),
        ): ((&[u8; 48], &Ulid), (&[u8; 48], &Ulid))| {
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

        // POS
        for edge_pair in self.pos_data.iter().zip(other.pos_data.iter()) {
            if !check_edge(edge_pair) {
                return false;
            }
        }

        // OSP
        for edge_pair in self.osp_data.iter().zip(other.osp_data.iter()) {
            if !check_edge(edge_pair) {
                return false;
            }
        }

        // If we have a different number of comparisons compared to the number of edges, we know that there are orphans
        if cached_comparisons.len() != self.edge_props.len()
            || cached_comparisons.len() != other.edge_props.len()
        {}

        true
    }
}

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    pub fn new() -> Self {
        Self {
            node_props: BTreeMap::new(),
            edge_props: BTreeMap::new(),
            spo_data: BTreeMap::new(),
            pos_data: BTreeMap::new(),
            osp_data: BTreeMap::new(),
        }
    }
}

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType> TripleStoreError
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type Error = ();
}
