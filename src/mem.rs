use crate::{MergableTripleStore, Mergeable, Query, Triple, TripleStore};
use std::collections::{BTreeMap, HashMap};
use ulid::Ulid;

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

impl<'a, NodeProperties: Clone + 'a, EdgeProperties: Clone + 'a>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    pub fn new() -> Self {
        Self {
            node_props: HashMap::new(),
            edge_props: HashMap::new(),
            spo_data: BTreeMap::new(),
            pos_data: BTreeMap::new(),
            osp_data: BTreeMap::new(),
        }
    }

    fn iter_spo(
        &'a self,
    ) -> impl Iterator<Item = crate::DecoratedTriple<&NodeProperties, &EdgeProperties>> {
        self.handle_iter_spo()
    }

    fn iter_pos(
        &'a self,
    ) -> impl Iterator<Item = crate::DecoratedTriple<&NodeProperties, &EdgeProperties>> {
        self.handle_iter_pos()
    }

    fn iter_osp(
        &'a self,
    ) -> impl Iterator<Item = crate::DecoratedTriple<&NodeProperties, &EdgeProperties>> {
        self.handle_iter_osp()
    }

    fn iter_node(&'a self) -> impl Iterator<Item = (&Ulid, &NodeProperties)> {
        self.handle_iter_node()
    }

    fn iter_edge_spo(&'a self) -> impl Iterator<Item = (Triple, &EdgeProperties)> {
        self.handle_iter_edge_spo()
    }

    fn iter_edge_pos(&'a self) -> impl Iterator<Item = (Triple, &EdgeProperties)> {
        self.handle_iter_edge_pos()
    }

    fn iter_edge_osp(&'a self) -> impl Iterator<Item = (Triple, &EdgeProperties)> {
        self.handle_iter_edge_osp()
    }
}

impl<NodeProperties: Clone, EdgeProperties: Clone> TripleStore<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type Error = ();
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;

    fn extend(&mut self, other: Self) {
        for (id, data) in other.node_props {
            self.node_props.insert(id, data);
        }

        for (id, data) in other.edge_props {
            self.edge_props.insert(id, data);
        }

        for (id, data) in other.spo_data {
            self.spo_data.insert(id, data);
        }

        for (id, data) in other.pos_data {
            self.pos_data.insert(id, data);
        }

        for (id, data) in other.osp_data {
            self.osp_data.insert(id, data);
        }
    }

    fn insert_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), Self::Error> {
        self.handle_insert_node(node, data)
    }

    fn insert_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (Ulid, NodeProperties)>,
    ) -> Result<(), Self::Error> {
        self.handle_insert_node_batch(nodes)
    }

    fn insert_edge(&mut self, triple: Triple, data: EdgeProperties) -> Result<(), Self::Error> {
        self.handle_insert_edge(triple, data)
    }

    fn insert_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (Triple, EdgeProperties)>,
    ) -> Result<(), Self::Error> {
        self.handle_insert_edge_batch(triples)
    }

    fn remove_node(&mut self, node: &Ulid) -> Result<(), Self::Error> {
        self.handle_remove_node(node)
    }

    fn remove_node_batch(&mut self, nodes: impl Iterator<Item = Ulid>) -> Result<(), Self::Error> {
        self.handle_remove_node_batch(nodes)
    }

    fn remove_edge(&mut self, triple: Triple) -> Result<(), Self::Error> {
        self.handle_remove_edge(triple)
    }

    fn remove_edge_batch(
        &mut self,
        triples: impl Iterator<Item = Triple>,
    ) -> Result<(), Self::Error> {
        self.handle_remove_edge_batch(triples)
    }

    fn query(
        &mut self,
        query: Query,
    ) -> Result<MemTripleStore<NodeProperties, EdgeProperties>, Self::Error> {
        self.handle_query(query)
    }

    // fn into_iter_spo(self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     self.handle_into_iter_spo()
    // }

    // fn into_iter_pos(self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     self.handle_into_iter_pos()
    // }

    // fn into_iter_osp(self) -> impl Iterator<Item = crate::DecoratedTriple<NodeProperties, EdgeProperties>> {
    //     self.handle_into_iter_osp()
    // }

    // fn into_iter_node(self) -> impl Iterator<Item = (Ulid, NodeProperties)> {
    //     self.handle_into_iter_node()
    // }

    // fn into_iter_edge_spo(self) -> impl Iterator<Item = (Triple, EdgeProperties)> {
    //     self.handle_into_iter_edge_spo()
    // }

    // fn into_iter_edge_pos(self) -> impl Iterator<Item = (Triple, EdgeProperties)> {
    //     self.handle_into_iter_edge_pos()
    // }

    // fn into_iter_edge_osp(self) -> impl Iterator<Item = (Triple, EdgeProperties)> {
    //     self.handle_into_iter_edge_osp()
    // }
}

impl<NodeProperties: Clone + Mergeable, EdgeProperties: Clone + Mergeable>
    MergableTripleStore<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn merge(&mut self, other: Self) {
        for (id, data) in other.node_props {
            match self.node_props.entry(id) {
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(data);
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(data);
                }
            }
        }

        for (id, data) in other.edge_props {
            match self.edge_props.entry(id) {
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(data);
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(data);
                }
            }
        }

        for (id, data) in other.spo_data {
            self.spo_data.insert(id, data);
        }

        for (id, data) in other.pos_data {
            self.pos_data.insert(id, data);
        }

        for (id, data) in other.osp_data {
            self.osp_data.insert(id, data);
        }
    }

    fn merge_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), Self::Error> {
        self.handle_merge_node(node, data)
    }

    fn merge_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (Ulid, NodeProperties)>,
    ) -> Result<(), Self::Error> {
        self.handle_merge_node_batch(nodes)
    }

    fn merge_edge(&mut self, triple: Triple, data: EdgeProperties) -> Result<(), Self::Error> {
        self.handle_merge_edge(triple, data)
    }

    fn merge_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (Triple, EdgeProperties)>,
    ) -> Result<(), Self::Error> {
        self.handle_merge_edge_batch(triples)
    }
}
