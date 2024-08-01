use std::borrow::Borrow;

use crate::{
    prelude::*,
    traits::{ConcreteIdType, Property},
    Triple,
};

use super::MemTripleStore;

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property>
    MemTripleStore<Id, NodeProps, EdgeProps>
{
    // Gets the set of outgoing edges from a given node.
    fn get_spo_edge_range(&self, node: &Id) -> (Vec<Triple<Id>>, Vec<Id>) {
        self.spo_data.range(Id::key_bounds_1(*node)).fold(
            (Vec::new(), Vec::new()),
            |(mut triples, mut edge_data_ids), (triple, edge_data_id)| {
                triples.push(Id::decode_spo_triple(triple));
                edge_data_ids.push(edge_data_id.clone());
                (triples, edge_data_ids)
            },
        )
    }

    // Gets the set of incoming edges to a given node.
    fn get_osp_edge_range(&self, node: &Id) -> (Vec<Triple<Id>>, Vec<Id>) {
        self.osp_data.range(Id::key_bounds_1(*node)).fold(
            (Vec::new(), Vec::new()),
            |(mut triples, mut edge_data_ids), (triple, edge_data_id)| {
                triples.push(Id::decode_osp_triple(triple));
                edge_data_ids.push(edge_data_id.clone());
                (triples, edge_data_ids)
            },
        )
    }
}

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property>
    TripleStoreRemove<Id, NodeProps, EdgeProps> for MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn remove_node(&mut self, node: impl Borrow<Id>) -> Result<(), Self::Error> {
        // Find all uses of this node in the edges.
        let (forward_triples, forward_edge_data_ids) = self.get_spo_edge_range(node.borrow());
        let (backward_triples, backward_edge_data_ids) = self.get_osp_edge_range(node.borrow());

        // Remove the node props.
        self.node_props.remove(node.borrow());

        // Remove all the edge props for all the edges we'll be removing.
        for edge_data_id in forward_edge_data_ids
            .into_iter()
            .chain(backward_edge_data_ids.into_iter())
        {
            self.edge_props.remove(&edge_data_id);
        }

        // Remove the forward and backward edges
        for edge in forward_triples
            .into_iter()
            .chain(backward_triples.into_iter())
        {
            self.remove_edge(edge)?;
        }

        Ok(())
    }

    fn remove_edge(&mut self, triple: Triple<Id>) -> Result<(), Self::Error> {
        if let std::collections::btree_map::Entry::Occupied(spo_data_entry) =
            self.spo_data.entry(Id::encode_spo_triple(&triple))
        {
            // Remove the edge from the 3 orderings.
            let edge_data_id = spo_data_entry.remove();
            self.pos_data.remove(&Id::encode_pos_triple(&triple));
            self.osp_data.remove(&Id::encode_osp_triple(&triple));

            // Clean up the edge props.
            self.edge_props.remove(&edge_data_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{MemTripleStore, UlidIdGenerator};

    #[test]
    fn test_remove_node() {
        crate::conformance::remove::test_remove_node(MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_remove_edge() {
        crate::conformance::remove::test_remove_edge(MemTripleStore::new(UlidIdGenerator::new()));
    }
}
