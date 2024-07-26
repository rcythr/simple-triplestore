use ulid::Ulid;

use crate::{Triple, TripleStoreRemove};

use super::MemTripleStore;

impl<NodeProperties: Clone, EdgeProperties: Clone> MemTripleStore<NodeProperties, EdgeProperties> {
    // Gets the set of outgoing edges from a given node.
    fn get_spo_edge_range(&self, node: &Ulid) -> (Vec<Triple>, Vec<Ulid>) {
        self.spo_data
            .range((
                std::ops::Bound::Included(
                    Triple {
                        sub: node.clone(),
                        pred: Ulid(u128::MIN),
                        obj: Ulid(u128::MIN),
                    }
                    .encode_spo(),
                ),
                std::ops::Bound::Included(
                    Triple {
                        sub: node.clone(),
                        pred: Ulid(u128::MAX),
                        obj: Ulid(u128::MAX),
                    }
                    .encode_spo(),
                ),
            ))
            .fold(
                (Vec::new(), Vec::new()),
                |(mut triples, mut edge_data_ids), (triple, edge_data_id)| {
                    triples.push(Triple::decode_spo(triple));
                    edge_data_ids.push(edge_data_id.clone());
                    (triples, edge_data_ids)
                },
            )
    }

    // Gets the set of incoming edges to a given node.
    fn get_osp_edge_range(&self, node: &Ulid) -> (Vec<Triple>, Vec<Ulid>) {
        self.osp_data
            .range((
                std::ops::Bound::Included(
                    Triple {
                        sub: Ulid(u128::MIN),
                        pred: Ulid(u128::MIN),
                        obj: node.clone(),
                    }
                    .encode_osp(),
                ),
                std::ops::Bound::Included(
                    Triple {
                        sub: Ulid(u128::MAX),
                        pred: Ulid(u128::MAX),
                        obj: node.clone(),
                    }
                    .encode_osp(),
                ),
            ))
            .fold(
                (Vec::new(), Vec::new()),
                |(mut triples, mut edge_data_ids), (triple, edge_data_id)| {
                    triples.push(Triple::decode_osp(triple));
                    edge_data_ids.push(edge_data_id.clone());
                    (triples, edge_data_ids)
                },
            )
    }

    pub(super) fn handle_remove_node(&mut self, node: &Ulid) -> Result<(), ()> {
        // Find all uses of this node in the edges.
        let (forward_triples, forward_edge_data_ids) = self.get_spo_edge_range(node);
        let (backward_triples, backward_edge_data_ids) = self.get_osp_edge_range(node);

        // Remove the node props.
        self.node_props.remove(&node);

        // Remove all the edge props for all the edges we'll be removing.
        for edge_data_id in forward_edge_data_ids
            .into_iter()
            .chain(backward_edge_data_ids.into_iter())
        {
            self.edge_props.remove(&edge_data_id);
        }

        // Remove the forward and backward edges
        self.handle_remove_edge_batch(
            forward_triples
                .into_iter()
                .chain(backward_triples.into_iter()),
        )?;

        Ok(())
    }

    pub(super) fn handle_remove_node_batch(
        &mut self,
        nodes: impl Iterator<Item = Ulid>,
    ) -> Result<(), ()> {
        for node in nodes {
            self.handle_remove_node(&node)?;
        }
        Ok(())
    }

    pub(super) fn handle_remove_edge(&mut self, triple: Triple) -> Result<(), ()> {
        if let std::collections::btree_map::Entry::Occupied(spo_data_entry) =
            self.spo_data.entry(triple.encode_spo())
        {
            // Remove the edge from the 3 orderings.
            let edge_data_id = spo_data_entry.remove();
            self.pos_data.remove(&triple.encode_pos());
            self.osp_data.remove(&triple.encode_osp());

            // Clean up the edge props.
            self.edge_props.remove(&edge_data_id);
        }
        Ok(())
    }

    pub(super) fn handle_remove_edge_batch(
        &mut self,
        triples: impl Iterator<Item = Triple>,
    ) -> Result<(), ()> {
        for triple in triples {
            self.handle_remove_edge(triple)?;
        }
        Ok(())
    }
}

impl<NodeProperties: Clone, EdgeProperties: Clone> TripleStoreRemove<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
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
}
