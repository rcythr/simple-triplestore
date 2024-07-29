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
    use ulid::Ulid;

    use crate::{MemTripleStore, Triple, TripleStoreInsert};

    #[test]
    fn test_insert_node() {
        // foo   bar   baz   quz

        let mut db: MemTripleStore<String, String> = MemTripleStore::new();

        let (node_1, data_1) = (Ulid(1), "foo".to_string());
        let (node_2, data_2) = (Ulid(2), "bar".to_string());
        let (node_3, data_3) = (Ulid(3), "baz".to_string());
        let (node_4, data_4) = (Ulid(4), "quz".to_string());
        let data_5 = "quz2".to_string();

        db.insert_node(node_1, data_1.clone())
            .expect("Insert should succeed");
        db.insert_node(node_2, data_2.clone())
            .expect("Insert should succeed");
        db.insert_node(node_3, data_3.clone())
            .expect("Insert should succeed");
        db.insert_node(node_4, data_4.clone())
            .expect("Insert should succeed");

        assert_eq!(db.node_props.len(), 4);
        assert_eq!(
            *db.node_props.get(&node_1).expect("node_1 should be found"),
            data_1
        );
        assert_eq!(
            *db.node_props.get(&node_2).expect("node_2 should be found"),
            data_2
        );
        assert_eq!(
            *db.node_props.get(&node_3).expect("node_3 should be found"),
            data_3
        );
        assert_eq!(
            *db.node_props.get(&node_4).expect("node_4 should be found"),
            data_4
        );
        // Update one of the entries by replacement.
        db.insert_node(node_4.clone(), data_5.clone())
            .expect("Insert should succeed");
        assert_eq!(
            *db.node_props.get(&node_4).expect("node_4 should be found"),
            data_5
        );
        assert_eq!(db.edge_props.len(), 0);
        assert_eq!(db.spo_data.len(), 0);
        assert_eq!(db.pos_data.len(), 0);
        assert_eq!(db.osp_data.len(), 0);
    }

    #[test]
    fn test_insert_node_batch() {
        // foo   bar   baz   quz

        let mut db: MemTripleStore<String, String> = MemTripleStore::new();

        let (node_1, data_1) = (Ulid(1), "foo".to_string());
        let (node_2, data_2) = (Ulid(2), "bar".to_string());
        let (node_3, data_3) = (Ulid(3), "baz".to_string());
        let (node_4, data_4) = (Ulid(4), "quz".to_string());
        let data_5 = "quz2".to_string();

        db.insert_node_batch([
            (node_1, data_1.clone()),
            (node_2, data_2.clone()),
            (node_3, data_3.clone()),
            (node_4, data_4.clone()),
            // Clobber the earlier entry
            (node_4, data_5.clone()),
        ])
        .expect("insert should succeed");

        assert_eq!(db.node_props.len(), 4);
        assert_eq!(
            *db.node_props.get(&node_1).expect("node_1 should be found"),
            data_1
        );
        assert_eq!(
            *db.node_props.get(&node_2).expect("node_2 should be found"),
            data_2
        );
        assert_eq!(
            *db.node_props.get(&node_3).expect("node_3 should be found"),
            data_3
        );
        assert_eq!(
            *db.node_props.get(&node_4).expect("node_4 should be found"),
            data_5
        );
        assert_eq!(db.edge_props.len(), 0);
        assert_eq!(db.spo_data.len(), 0);
        assert_eq!(db.pos_data.len(), 0);
        assert_eq!(db.osp_data.len(), 0);
    }

    #[test]
    fn test_insert_edge() {
        // foo -1-> bar -2-> baz -3-> quz

        let mut db: MemTripleStore<String, String> = MemTripleStore::new();

        let (node_1, node_data_1) = (Ulid(1), "foo".to_string());
        let (node_2, node_data_2) = (Ulid(2), "bar".to_string());
        let (node_3, node_data_3) = (Ulid(3), "baz".to_string());
        let (node_4, node_data_4) = (Ulid(4), "quz".to_string());

        db.insert_node_batch([
            (node_1, node_data_1.clone()),
            (node_2, node_data_2.clone()),
            (node_3, node_data_3.clone()),
            (node_4, node_data_4.clone()),
        ])
        .expect("insert should succeed");

        let (edge_1, edge_data_1) = (Ulid(1), "-1->".to_string());
        let (edge_2, edge_data_2) = (Ulid(2), "-2->".to_string());
        let (edge_3, edge_data_3) = (Ulid(3), "-3->".to_string());
        let edge_data_4 = "-4->".to_string();

        db.insert_edge(
            Triple {
                sub: node_1,
                pred: edge_1,
                obj: node_2,
            },
            edge_data_1.clone(),
        )
        .expect("insert edge should succeed");
        db.insert_edge(
            Triple {
                sub: node_2,
                pred: edge_2,
                obj: node_3,
            },
            edge_data_2.clone(),
        )
        .expect("insert edge should succeed");
        db.insert_edge(
            Triple {
                sub: node_3,
                pred: edge_3,
                obj: node_4,
            },
            edge_data_3.clone(),
        )
        .expect("insert edge should succeed");

        // Update one of the edges
        db.insert_edge(
            Triple {
                sub: node_3,
                pred: edge_3,
                obj: node_4,
            },
            edge_data_4.clone(),
        )
        .expect("insert edge should succeed");

        assert_eq!(db.node_props.len(), 4);
        assert_eq!(
            *db.node_props.get(&node_1).expect("node_1 should be found"),
            node_data_1
        );
        assert_eq!(
            *db.node_props.get(&node_2).expect("node_2 should be found"),
            node_data_2
        );
        assert_eq!(
            *db.node_props.get(&node_3).expect("node_3 should be found"),
            node_data_3
        );
        assert_eq!(
            *db.node_props.get(&node_4).expect("node_4 should be found"),
            node_data_4
        );

        assert_eq!(db.edge_props.len(), 3);
        assert_eq!(
            db.edge_props
                .into_values()
                .collect::<std::collections::HashSet<_>>(),
            [edge_data_1, edge_data_2, edge_data_4].into()
        );
        assert_eq!(db.spo_data.len(), 3);
        assert_eq!(db.pos_data.len(), 3);
        assert_eq!(db.osp_data.len(), 3);
    }

    #[test]
    fn test_insert_edge_batch() {
        // foo -1-> bar -2-> baz -3-> quz

        let mut db: MemTripleStore<String, String> = MemTripleStore::new();

        let (node_1, node_data_1) = (Ulid(1), "foo".to_string());
        let (node_2, node_data_2) = (Ulid(2), "bar".to_string());
        let (node_3, node_data_3) = (Ulid(3), "baz".to_string());
        let (node_4, node_data_4) = (Ulid(4), "quz".to_string());

        db.insert_node_batch([
            (node_1, node_data_1.clone()),
            (node_2, node_data_2.clone()),
            (node_3, node_data_3.clone()),
            (node_4, node_data_4.clone()),
        ])
        .expect("insert should succeed");

        let (edge_1, edge_data_1) = (Ulid(1), "-1->".to_string());
        let (edge_2, edge_data_2) = (Ulid(2), "-2->".to_string());
        let (edge_3, edge_data_3) = (Ulid(3), "-3->".to_string());

        db.insert_edge_batch([
            (
                Triple {
                    sub: node_1,
                    pred: edge_1,
                    obj: node_2,
                },
                edge_data_1.clone(),
            ),
            (
                Triple {
                    sub: node_2,
                    pred: edge_2,
                    obj: node_3,
                },
                edge_data_2.clone(),
            ),
            (
                Triple {
                    sub: node_3,
                    pred: edge_3,
                    obj: node_4,
                },
                edge_data_3.clone(),
            ),
        ])
        .expect("insert_edge_batch should work");

        assert_eq!(db.node_props.len(), 4);
        assert_eq!(
            *db.node_props.get(&node_1).expect("node_1 should be found"),
            node_data_1
        );
        assert_eq!(
            *db.node_props.get(&node_2).expect("node_2 should be found"),
            node_data_2
        );
        assert_eq!(
            *db.node_props.get(&node_3).expect("node_3 should be found"),
            node_data_3
        );
        assert_eq!(
            *db.node_props.get(&node_4).expect("node_4 should be found"),
            node_data_4
        );
        assert_eq!(db.edge_props.len(), 3);
        assert_eq!(
            db.edge_props
                .into_values()
                .collect::<std::collections::HashSet<_>>(),
            [edge_data_1, edge_data_2, edge_data_3].into()
        );
        assert_eq!(db.spo_data.len(), 3);
        assert_eq!(db.pos_data.len(), 3);
        assert_eq!(db.osp_data.len(), 3);
    }
}
