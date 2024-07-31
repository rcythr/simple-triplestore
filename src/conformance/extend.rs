use crate::prelude::*;
use ulid::Ulid;

struct Config {
    node_1: Ulid,
    node_props_1: String,
    node_2: Ulid,
    node_props_2: String,
    node_3: Ulid,
    node_props_3: String,
    node_4: Ulid,
    node_props_4: String,
    edge_1: Ulid,
    edge_props_1: String,
    edge_props_2: String,
}

impl Default for Config {
    fn default() -> Self {
        let (node_1, node_props_1) = (Ulid::new(), "a".to_string());
        let (node_2, node_props_2) = (Ulid::new(), "b".to_string());
        let (node_3, node_props_3) = (Ulid::new(), "c".to_string());
        let (node_4, node_props_4) = (node_1, "d".to_string());

        let edge_1 = Ulid::new();
        let edge_props_1 = "1".to_string();
        let edge_props_2 = "2".to_string();

        Self {
            node_1,
            node_2,
            node_3,
            node_4,
            node_props_1,
            node_props_2,
            node_props_3,
            node_props_4,
            edge_1,
            edge_props_1,
            edge_props_2,
        }
    }
}

fn setup_left<T: TripleStore<Ulid, String, String>>(config: &Config, left: &mut T) {
    // Construct the left graph to be (1, "a") -("1")-> (2, "b")
    left.insert_node(config.node_1, config.node_props_1.clone())
        .expect("success");
    left.insert_node(config.node_2, config.node_props_2.clone())
        .expect("success");
    left.insert_edge(
        Triple {
            sub: config.node_1,
            pred: config.edge_1,
            obj: config.node_2,
        },
        config.edge_props_1.clone(),
    )
    .expect("success");
}

fn setup_right<T: TripleStore<Ulid, String, String>>(config: &Config, right: &mut T) {
    // Construct the right graph to be (3, "c") -("2")-> (1, "d")
    right
        .insert_node(config.node_3, config.node_props_3.clone())
        .expect("success");
    right
        .insert_node(config.node_4, config.node_props_4.clone())
        .expect("success");
    right
        .insert_edge(
            Triple {
                sub: config.node_3,
                pred: config.edge_1,
                obj: config.node_4,
            },
            config.edge_props_2.clone(),
        )
        .expect("success");
}

pub(crate) fn test_extend<T: TripleStore<Ulid, String, String>>(mut left: T, mut right: T) {
    let config = Config::default();
    setup_left(&config, &mut left);
    setup_right(&config, &mut right);

    // Perform the extension.
    left.extend(right).expect("success");

    // We expect the result to be (3, "c") -("2")-> (1, "d") -("1")-> (2, "b")
    let node_data = left
        .iter_vertices()
        .map(|i| i.expect("success"))
        .collect::<Vec<_>>();
    assert_eq!(node_data.len(), 3);
    assert!(node_data.contains(&(config.node_1, config.node_props_4)));
    assert!(node_data.contains(&(config.node_2, config.node_props_2)));
    assert!(node_data.contains(&(config.node_3, config.node_props_3)));

    let edge_data = left
        .iter_edges(crate::EdgeOrder::SPO)
        .map(|i| i.expect("success"))
        .collect::<Vec<_>>();
    assert_eq!(edge_data.len(), 2);
    assert!(edge_data.contains(&(
        Triple {
            sub: config.node_3,
            pred: config.edge_1,
            obj: config.node_1
        },
        config.edge_props_2
    )));
    assert!(edge_data.contains(&(
        Triple {
            sub: config.node_1,
            pred: config.edge_1,
            obj: config.node_2
        },
        config.edge_props_1
    )));
}
