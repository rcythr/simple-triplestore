use crate::TripleStoreExtend;

use super::MemTripleStore;

impl<NodeProperties: Clone, EdgeProperties: Clone> TripleStoreExtend<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn extend(&mut self, other: Self) -> Result<(), ()> {
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

        Ok(())
    }
}
