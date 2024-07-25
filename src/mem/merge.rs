use ulid::Ulid;

use crate::{Mergeable, Triple};

use super::MemTripleStore;

impl<NodeProperties: Clone + Mergeable, EdgeProperties: Clone + Mergeable>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    pub(super) fn handle_merge_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), ()> {
        match self.node_props.entry(node) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                o.get_mut().merge(data);
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                v.insert(data);
            }
        }
        Ok(())
    }

    pub(super) fn handle_merge_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (Ulid, NodeProperties)>,
    ) -> Result<(), ()> {
        for (node, data) in nodes {
            self.handle_merge_node(node, data)?;
        }
        Ok(())
    }

    fn merge_edge_create_data(
        &mut self,
        old_edge_data_id: Option<Ulid>,
        new_edge_data: EdgeProperties,
    ) -> Ulid {
        if let Some(old_edge_data_id) = old_edge_data_id {
            match self.edge_props.entry(old_edge_data_id.clone()) {
                std::collections::hash_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(new_edge_data)
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(new_edge_data);
                }
            }
            old_edge_data_id
        } else {
            self.insert_edge_create_data(&old_edge_data_id, new_edge_data)
        }
    }

    pub(super) fn handle_merge_edge(
        &mut self,
        triple: Triple,
        data: EdgeProperties,
    ) -> Result<(), ()> {
        let old_edge_data_id = match self.spo_data.entry(Triple::encode_spo(&triple)) {
            std::collections::btree_map::Entry::Vacant(_) => None,
            std::collections::btree_map::Entry::Occupied(o) => Some(o.get().clone()),
        };

        let new_edge_data_id = self.merge_edge_create_data(old_edge_data_id, data);

        self.insert_edge_data_internal(&triple, &new_edge_data_id);

        Ok(())
    }

    pub(super) fn handle_merge_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (Triple, EdgeProperties)>,
    ) -> Result<(), ()> {
        for (triple, data) in triples {
            self.handle_merge_edge(triple, data)?;
        }
        Ok(())
    }
}
