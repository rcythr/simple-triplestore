use crate::{prelude::*, traits::IdType, traits::Property, Triple};

use super::MemTripleStore;

impl<Id: IdType, NodeProps: Property, EdgeProps: Property>
    MemTripleStore<Id, NodeProps, EdgeProps>
{
    pub(super) fn insert_edge_data_internal(&mut self, triple: &Triple<Id>, new_edge_data_id: &Id) {
        self.spo_data
            .insert(Id::encode_spo_triple(&triple), new_edge_data_id.clone());
        self.pos_data
            .insert(Id::encode_pos_triple(&triple), new_edge_data_id.clone());
        self.osp_data
            .insert(Id::encode_osp_triple(&triple), new_edge_data_id.clone());
    }

    /// Handles the case where we are treating the edge data as new for the first time.
    /// Either because it is, or because we're not using merge semantics.
    pub(super) fn insert_edge_create_data(
        &mut self,
        old_edge_data_id: &Option<Id>,
        new_edge_data: EdgeProps,
    ) -> Id {
        // Clean up the old data.
        if let Some(old_edge_data_id) = old_edge_data_id {
            self.edge_props.remove(&old_edge_data_id);
        }

        // Insert the new data with a fresh Ulid.
        let new_edge_data_id = self.id_generator.fresh();
        self.edge_props.insert(new_edge_data_id, new_edge_data);
        new_edge_data_id
    }
}

impl<Id: IdType, NodeProps: Property, EdgeProps: Property>
    TripleStoreInsert<Id, NodeProps, EdgeProps> for MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn insert_node(&mut self, node: Id, data: NodeProps) -> Result<(), Self::Error> {
        match self.node_props.entry(node) {
            std::collections::btree_map::Entry::Occupied(mut o) => {
                o.insert(data);
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(data);
            }
        }
        Ok(())
    }

    fn insert_edge(&mut self, triple: Triple<Id>, data: EdgeProps) -> Result<(), Self::Error> {
        let old_edge_data_id = match self.spo_data.entry(Id::encode_spo_triple(&triple)) {
            std::collections::btree_map::Entry::Vacant(_) => None,
            std::collections::btree_map::Entry::Occupied(o) => Some(o.get().clone()),
        };

        let new_edge_data_id = self.insert_edge_create_data(&old_edge_data_id, data);

        self.insert_edge_data_internal(&triple, &new_edge_data_id);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{MemTripleStore, UlidIdGenerator};
    use ulid::Ulid;

    #[test]
    fn test_insert_node() {
        let db: MemTripleStore<Ulid, String, String> = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::insert::test_insert_node(db);
    }

    #[test]
    fn test_insert_edge() {
        let db: MemTripleStore<Ulid, String, String> = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::insert::test_insert_edge(db);
    }
}
