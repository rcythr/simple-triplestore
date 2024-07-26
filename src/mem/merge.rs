use ulid::Ulid;

use crate::{Mergeable, Triple, TripleStoreMerge};

use super::MemTripleStore;

impl<NodeProperties: Clone + Mergeable, EdgeProperties: Clone + Mergeable>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    fn merge_edge_create_data(
        &mut self,
        old_edge_data_id: Option<Ulid>,
        new_edge_data: EdgeProperties,
    ) -> Ulid {
        if let Some(old_edge_data_id) = old_edge_data_id {
            match self.edge_props.entry(old_edge_data_id.clone()) {
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(new_edge_data)
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(new_edge_data);
                }
            }
            old_edge_data_id
        } else {
            self.insert_edge_create_data(&old_edge_data_id, new_edge_data)
        }
    }
}

impl<NodeProperties: Clone + Mergeable, EdgeProperties: Clone + Mergeable>
    TripleStoreMerge<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn merge(&mut self, other: Self) {
        for (id, data) in other.node_props {
            match self.node_props.entry(id) {
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(data);
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(data);
                }
            }
        }

        for (id, data) in other.edge_props {
            match self.edge_props.entry(id) {
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(data);
                }
                std::collections::btree_map::Entry::Vacant(v) => {
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

    fn merge_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), ()> {
        match self.node_props.entry(node) {
            std::collections::btree_map::Entry::Occupied(mut o) => {
                o.get_mut().merge(data);
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(data);
            }
        }
        Ok(())
    }

    fn merge_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (Ulid, NodeProperties)>,
    ) -> Result<(), ()> {
        for (node, data) in nodes {
            self.merge_node(node, data)?;
        }
        Ok(())
    }

    fn merge_edge(&mut self, triple: Triple, data: EdgeProperties) -> Result<(), ()> {
        let old_edge_data_id = match self.spo_data.entry(Triple::encode_spo(&triple)) {
            std::collections::btree_map::Entry::Vacant(_) => None,
            std::collections::btree_map::Entry::Occupied(o) => Some(o.get().clone()),
        };

        let new_edge_data_id = self.merge_edge_create_data(old_edge_data_id, data);

        self.insert_edge_data_internal(&triple, &new_edge_data_id);

        Ok(())
    }

    fn merge_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (Triple, EdgeProperties)>,
    ) -> Result<(), ()> {
        for (triple, data) in triples {
            self.merge_edge(triple, data)?;
        }
        Ok(())
    }
}
