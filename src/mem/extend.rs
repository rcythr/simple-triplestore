use crate::{PropertiesType, TripleStoreExtend};

use super::MemTripleStore;

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>
    TripleStoreExtend<NodeProperties, EdgeProperties>
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

#[cfg(test)]
mod test {
    use ulid::Ulid;

    use crate::prelude::*;
    use crate::{MemTripleStore, Triple};

    #[test]
    fn test_extend() {
        let mut left = MemTripleStore::new();
        let mut right = MemTripleStore::new();

        let (node_1, node_props_1) = (Ulid::new(), "a".to_string());
        let (node_2, node_props_2) = (Ulid::new(), "b".to_string());
        let (node_3, node_props_3) = (Ulid::new(), "c".to_string());
        let (node_4, node_props_4) = (node_1, "d".to_string());

        let edge_1 = Ulid::new();
        let edge_props_1 = "1".to_string();
        let edge_props_2 = "2".to_string();

        // Construct the left graph to be (1, "a") -("1")-> (2, "b")
        left.insert_node(node_1.clone(), node_props_1.clone())
            .expect("success");
        left.insert_node(node_2.clone(), node_props_2.clone())
            .expect("success");
        left.insert_edge(
            Triple {
                sub: node_1,
                pred: edge_1,
                obj: node_2,
            },
            edge_props_1.clone(),
        )
        .expect("success");

        // Construct the right graph to be (3, "c") -("2")-> (1, "d")
        right
            .insert_node(node_3.clone(), node_props_3.clone())
            .expect("success");
        right
            .insert_node(node_4.clone(), node_props_4.clone())
            .expect("success");
        right
            .insert_edge(
                Triple {
                    sub: node_3,
                    pred: edge_1,
                    obj: node_4,
                },
                edge_props_2.clone(),
            )
            .expect("success");

        // Perform the extension.
        left.extend(right).expect("success");

        // We expect the result to be (3, "c") -("2")-> (1, "d") -("1")-> (2, "b")
        let node_data = left
            .iter_node()
            .map(|i| i.expect("success"))
            .collect::<Vec<_>>();
        assert_eq!(node_data.len(), 3);
        assert!(node_data.contains(&(node_1, node_props_4)));
        assert!(node_data.contains(&(node_2, node_props_2)));
        assert!(node_data.contains(&(node_3, node_props_3)));

        let edge_data = left
            .iter_edge_spo()
            .map(|i| i.expect("success"))
            .collect::<Vec<_>>();
        assert_eq!(edge_data.len(), 2);
        assert!(edge_data.contains(&(
            Triple {
                sub: node_3,
                pred: edge_1,
                obj: node_1
            },
            edge_props_2
        )));
        assert!(edge_data.contains(&(
            Triple {
                sub: node_1,
                pred: edge_1,
                obj: node_2
            },
            edge_props_1
        )));
    }
}
