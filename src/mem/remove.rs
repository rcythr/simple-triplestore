use ulid::Ulid;

use crate::Triple;

use super::MemTripleStore;

impl<NodeProperties: Clone, EdgeProperties: Clone> MemTripleStore<NodeProperties, EdgeProperties> {
    pub(super) fn handle_remove_node(&mut self, node: &Ulid) -> Result<(), ()> {
        let (triples, edge_data_ids) = self
            .spo_data
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
                    edge_data_ids.push(edge_data_id);
                    (triples, edge_data_ids)
                },
            );

        self.node_props.remove(&node);
        for edge_data_id in edge_data_ids {
            self.edge_props.remove(edge_data_id);
        }

        self.handle_remove_edge_batch(triples.into_iter())?;

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
        self.spo_data.remove(&triple.encode_spo());
        self.pos_data.remove(&triple.encode_pos());
        self.osp_data.remove(&triple.encode_osp());
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
