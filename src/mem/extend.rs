use ulid::Ulid;

use crate::{ExtendError, PropertyType, TripleStore, TripleStoreExtend};

use super::MemTripleStore;

impl<NodeProperties: PropertyType, EdgeProperties: PropertyType>
    TripleStoreExtend<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn extend<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), ExtendError<(), E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(crate::EdgeOrder::SPO);

        for r in other_nodes {
            let (id, data) = r.map_err(|e| ExtendError::Right(e))?;
            match self.node_props.entry(id) {
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    *o.get_mut() = data;
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(data);
                }
            }
        }

        for r in other_edges {
            let (id, other_edge_props) = r.map_err(|e| ExtendError::Right(e))?;

            match self.spo_data.entry(id.encode_spo()) {
                std::collections::btree_map::Entry::Vacant(self_spo_data_v) => {
                    // We don't have this edge already.
                    let other_edge_props_id = Ulid::new();

                    self_spo_data_v.insert(other_edge_props_id);
                    self.pos_data.insert(id.encode_pos(), other_edge_props_id);
                    self.osp_data.insert(id.encode_osp(), other_edge_props_id);
                    self.edge_props
                        .insert(other_edge_props_id, other_edge_props);
                }

                std::collections::btree_map::Entry::Occupied(self_spo_data_o) => {
                    let self_edge_props_id = self_spo_data_o.get();

                    let self_edge_data = self.edge_props.entry(*self_edge_props_id);

                    // Merge our edge props using the existing id.

                    match self_edge_data {
                        std::collections::btree_map::Entry::Vacant(v) => {
                            v.insert(other_edge_props);
                        }

                        std::collections::btree_map::Entry::Occupied(mut self_o) => {
                            *self_o.get_mut() = other_edge_props;
                        }
                    }
                }
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_extend() {
        let left = MemTripleStore::new();
        let right = MemTripleStore::new();
        crate::conformance::extend::test_extend(left, right);
    }
}
