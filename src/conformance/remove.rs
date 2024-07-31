use ulid::Ulid;

use crate::{prelude::*, EdgeOrder, PropsTriple, Triple};

struct Config {
    node_1: Ulid,
    node_data_1: String,
    node_2: Ulid,
    node_data_2: String,
    node_3: Ulid,
    node_data_3: String,
    node_4: Ulid,
    node_data_4: String,
    edge_1: Ulid,
    edge_data_1: String,
    edge_2: Ulid,
    edge_data_2: String,
    edge_3: Ulid,
    edge_data_3: String,
}

impl Default for Config {
    fn default() -> Self {
        let (node_1, node_data_1) = (Ulid(1), "foo".to_string());
        let (node_2, node_data_2) = (Ulid(2), "bar".to_string());
        let (node_3, node_data_3) = (Ulid(3), "baz".to_string());
        let (node_4, node_data_4) = (Ulid(4), "quz".to_string());
        let (edge_1, edge_data_1) = (Ulid(10), "-1->".to_string());
        let (edge_2, edge_data_2) = (Ulid(11), "-2->".to_string());
        let (edge_3, edge_data_3) = (Ulid(12), "-3->".to_string());

        Self {
            node_1,
            node_2,
            node_3,
            node_4,
            node_data_1,
            node_data_2,
            node_data_3,
            node_data_4,
            edge_1,
            edge_2,
            edge_3,
            edge_data_1,
            edge_data_2,
            edge_data_3,
        }
    }
}

fn populate_graph<T: TripleStore<Ulid, String, String>>(config: &Config, db: &mut T) {
    for (node, props) in [
        (config.node_1, config.node_data_1.clone()),
        (config.node_2, config.node_data_2.clone()),
        (config.node_3, config.node_data_3.clone()),
        (config.node_4, config.node_data_4.clone()),
    ] {
        db.insert_node(node, props).expect("insert should succeed");
    }

    for (triple, props) in [
        (
            Triple {
                sub: config.node_1,
                pred: config.edge_1,
                obj: config.node_2,
            },
            config.edge_data_1.clone(),
        ),
        (
            Triple {
                sub: config.node_2,
                pred: config.edge_2,
                obj: config.node_3,
            },
            config.edge_data_2.clone(),
        ),
        (
            Triple {
                sub: config.node_3,
                pred: config.edge_3,
                obj: config.node_4,
            },
            config.edge_data_3.clone(),
        ),
    ] {
        db.insert_edge(triple, props)
            .expect("insert_edge_batch should work");
    }
}

pub(crate) fn test_remove_node<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();
    populate_graph(&config, &mut db);

    db.remove_node(config.node_1).expect("ok");
    db.remove_node(config.node_4).expect("ok");

    assert_eq!(
        db.iter_vertices()
            .map(|r| r.expect("ok"))
            .collect::<Vec<_>>(),
        [
            (config.node_2, config.node_data_2.clone()),
            (config.node_3, config.node_data_3.clone()),
        ]
        .to_vec()
    );

    assert_eq!(
        db.iter_edges(EdgeOrder::SPO)
            .map(|r| r.expect("ok"))
            .collect::<Vec<_>>(),
        [(
            Triple {
                sub: config.node_2,
                pred: config.edge_2,
                obj: config.node_3,
            },
            config.edge_data_2.clone(),
        ),]
        .to_vec()
    );

    assert_eq!(
        db.iter_edges_with_props(EdgeOrder::SPO)
            .map(|r| r.expect("ok"))
            .collect::<Vec<_>>(),
        [PropsTriple {
            sub: (config.node_2, config.node_data_2),
            pred: (config.edge_2, config.edge_data_2),
            obj: (config.node_3, config.node_data_3)
        }]
        .to_vec()
    );
}

pub(crate) fn test_remove_edge<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();
    populate_graph(&config, &mut db);

    db.remove_edge(Triple {
        sub: config.node_1,
        pred: config.edge_1,
        obj: config.node_2,
    })
    .expect("ok");

    db.remove_edge(Triple {
        sub: config.node_3,
        pred: config.edge_3,
        obj: config.node_4,
    })
    .expect("ok");

    assert_eq!(
        db.iter_vertices()
            .map(|r| r.expect("ok"))
            .collect::<Vec<_>>(),
        [
            (config.node_1, config.node_data_1.clone()),
            (config.node_2, config.node_data_2.clone()),
            (config.node_3, config.node_data_3.clone()),
            (config.node_4, config.node_data_4.clone()),
        ]
        .to_vec()
    );

    assert_eq!(
        db.iter_edges(EdgeOrder::SPO)
            .map(|r| r.expect("ok"))
            .collect::<Vec<_>>(),
        [(
            Triple {
                sub: config.node_2,
                pred: config.edge_2,
                obj: config.node_3,
            },
            config.edge_data_2.clone(),
        )]
        .to_vec()
    );

    assert_eq!(
        db.iter_edges_with_props(EdgeOrder::SPO)
            .map(|r| r.expect("ok"))
            .collect::<Vec<_>>(),
        [PropsTriple {
            sub: (config.node_2, config.node_data_2),
            pred: (config.edge_2, config.edge_data_2),
            obj: (config.node_3, config.node_data_3)
        }]
        .to_vec()
    );
}
