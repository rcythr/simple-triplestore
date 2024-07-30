use ulid::Ulid;

use crate::{PropertyType, Triple, TripleStoreInsert};

use super::MemTripleStore;

impl<NodeProperties: PropertyType, EdgeProperties: PropertyType>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    pub(super) fn insert_edge_data_internal(&mut self, triple: &Triple, new_edge_data_id: &Ulid) {
        self.spo_data
            .insert(Triple::encode_spo(&triple), new_edge_data_id.clone());
        self.pos_data
            .insert(Triple::encode_pos(&triple), new_edge_data_id.clone());
        self.osp_data
            .insert(Triple::encode_osp(&triple), new_edge_data_id.clone());
    }

    /// Handles the case where we are treating the edge data as new for the first time.
    /// Either because it is, or because we're not using merge semantics.
    pub(super) fn insert_edge_create_data(
        &mut self,
        old_edge_data_id: &Option<Ulid>,
        new_edge_data: EdgeProperties,
    ) -> Ulid {
        // Clean up the old data.
        if let Some(old_edge_data_id) = old_edge_data_id {
            self.edge_props.remove(&old_edge_data_id);
        }

        // Insert the new data with a fresh Ulid.
        let new_edge_data_id = Ulid::new();
        self.edge_props.insert(new_edge_data_id, new_edge_data);
        new_edge_data_id
    }
}

impl<NodeProperties: PropertyType, EdgeProperties: PropertyType>
    TripleStoreInsert<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn insert_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), Self::Error> {
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

    fn insert_node_batch<I: IntoIterator<Item = (Ulid, NodeProperties)>>(
        &mut self,
        nodes: I,
    ) -> Result<(), Self::Error> {
        for (node, data) in nodes {
            self.insert_node(node, data)?;
        }
        Ok(())
    }

    fn insert_edge(&mut self, triple: Triple, data: EdgeProperties) -> Result<(), Self::Error> {
        let old_edge_data_id = match self.spo_data.entry(Triple::encode_spo(&triple)) {
            std::collections::btree_map::Entry::Vacant(_) => None,
            std::collections::btree_map::Entry::Occupied(o) => Some(o.get().clone()),
        };

        let new_edge_data_id = self.insert_edge_create_data(&old_edge_data_id, data);

        self.insert_edge_data_internal(&triple, &new_edge_data_id);

        Ok(())
    }

    fn insert_edge_batch<I: IntoIterator<Item = (Triple, EdgeProperties)>>(
        &mut self,
        triples: I,
    ) -> Result<(), Self::Error> {
        for (triple, data) in triples {
            self.insert_edge(triple, data)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::prelude::*;

    #[test]
    fn test_insert_node() {
        let db: MemTripleStore<String, String> = MemTripleStore::new();
        crate::conformance::insert::test_insert_node(db);
    }

    #[test]
    fn test_insert_node_batch() {
        let db: MemTripleStore<String, String> = MemTripleStore::new();
        crate::conformance::insert::test_insert_node_batch(db);
    }

    #[test]
    fn test_insert_edge() {
        let db: MemTripleStore<String, String> = MemTripleStore::new();
        crate::conformance::insert::test_insert_edge(db);
    }

    #[test]
    fn test_insert_edge_batch() {
        let db: MemTripleStore<String, String> = MemTripleStore::new();
        crate::conformance::insert::test_insert_edge_batch(db);
    }
}
