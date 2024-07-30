use ulid::Ulid;

use crate::prelude::*;

struct Config {
    node_1: Ulid,
    node_data_1: String,
    node_2: Ulid,
    node_data_2: String,
    node_3: Ulid,
    node_data_3: String,
    node_4: Ulid,
    node_data_4: String,
    node_data_5: String,
    edge_1: Ulid,
    edge_data_1: String,
    edge_2: Ulid,
    edge_data_2: String,
    edge_3: Ulid,
    edge_data_3: String,
    edge_data_4: String,
}

impl Default for Config {
    fn default() -> Self {
        let (node_1, node_data_1) = (Ulid(1), "foo".to_string());
        let (node_2, node_data_2) = (Ulid(2), "bar".to_string());
        let (node_3, node_data_3) = (Ulid(3), "baz".to_string());
        let (node_4, node_data_4) = (Ulid(4), "quz".to_string());
        let node_data_5 = "quz2".to_string();
        let (edge_1, edge_data_1) = (Ulid(10), "-1->".to_string());
        let (edge_2, edge_data_2) = (Ulid(11), "-2->".to_string());
        let (edge_3, edge_data_3) = (Ulid(12), "-3->".to_string());
        let edge_data_4 = "-4->".to_string();

        Self {
            node_1,
            node_2,
            node_3,
            node_4,
            node_data_1,
            node_data_2,
            node_data_3,
            node_data_4,
            node_data_5,
            edge_1,
            edge_2,
            edge_3,
            edge_data_1,
            edge_data_2,
            edge_data_3,
            edge_data_4,
        }
    }
}

pub(crate) fn test_insert_node<T: TripleStore<String, String>>(mut db: T) {
    let config = Config::default();

    db.insert_node(config.node_1, config.node_data_1.clone())
        .expect("Insert should succeed");
    db.insert_node(config.node_2, config.node_data_2.clone())
        .expect("Insert should succeed");
    db.insert_node(config.node_3, config.node_data_3.clone())
        .expect("Insert should succeed");
    db.insert_node(config.node_4, config.node_data_4.clone())
        .expect("Insert should succeed");

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);

    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (config.node_1, config.node_data_1.clone()),
            (config.node_2, config.node_data_2.clone()),
            (config.node_3, config.node_data_3.clone()),
            (config.node_4, config.node_data_4.clone()),
        ]
        .to_vec()
    );
    assert_eq!(edges.collect::<Vec<_>>().len(), 0);

    // Update one of the entries by replacement.
    db.insert_node(config.node_4, config.node_data_5.clone())
        .expect("Insert should succeed");

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (config.node_1, config.node_data_1),
            (config.node_2, config.node_data_2),
            (config.node_3, config.node_data_3),
            (config.node_4, config.node_data_5),
        ]
        .to_vec()
    );
    assert_eq!(edges.collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_insert_node_batch<T: TripleStore<String, String>>(mut db: T) {
    let config = Config::default();

    db.insert_node_batch([
        (config.node_1, config.node_data_1.clone()),
        (config.node_2, config.node_data_2.clone()),
        (config.node_3, config.node_data_3.clone()),
        (config.node_4, config.node_data_4.clone()),
        // Clobber the earlier entry
        (config.node_4, config.node_data_5.clone()),
    ])
    .expect("insert should succeed");

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (config.node_1, config.node_data_1.clone()),
            (config.node_2, config.node_data_2.clone()),
            (config.node_3, config.node_data_3.clone()),
            (config.node_4, config.node_data_5.clone()),
        ]
        .to_vec()
    );
    assert_eq!(edges.collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_insert_edge<T: TripleStore<String, String>>(mut db: T) {
    let config = Config::default();

    db.insert_node_batch([
        (config.node_1, config.node_data_1.clone()),
        (config.node_2, config.node_data_2.clone()),
        (config.node_3, config.node_data_3.clone()),
        (config.node_4, config.node_data_4.clone()),
    ])
    .expect("insert should succeed");

    db.insert_edge(
        Triple {
            sub: config.node_1,
            pred: config.edge_1,
            obj: config.node_2,
        },
        config.edge_data_1.clone(),
    )
    .expect("insert edge should succeed");
    db.insert_edge(
        Triple {
            sub: config.node_2,
            pred: config.edge_2,
            obj: config.node_3,
        },
        config.edge_data_2.clone(),
    )
    .expect("insert edge should succeed");
    db.insert_edge(
        Triple {
            sub: config.node_3,
            pred: config.edge_3,
            obj: config.node_4,
        },
        config.edge_data_3.clone(),
    )
    .expect("insert edge should succeed");

    // Update one of the edges
    db.insert_edge(
        Triple {
            sub: config.node_3,
            pred: config.edge_3,
            obj: config.node_4,
        },
        config.edge_data_4.clone(),
    )
    .expect("insert edge should succeed");

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (config.node_1, config.node_data_1.clone()),
            (config.node_2, config.node_data_2.clone()),
            (config.node_3, config.node_data_3.clone()),
            (config.node_4, config.node_data_4.clone()),
        ]
        .to_vec()
    );
    assert_eq!(
        edges.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_data_1.clone()
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
                config.edge_data_4.clone(),
            )
        ]
        .to_vec()
    );
}

pub(crate) fn test_insert_edge_batch<T: TripleStore<String, String>>(mut db: T) {
    let config = Config::default();

    db.insert_node_batch([
        (config.node_1, config.node_data_1.clone()),
        (config.node_2, config.node_data_2.clone()),
        (config.node_3, config.node_data_3.clone()),
        (config.node_4, config.node_data_4.clone()),
    ])
    .expect("insert should succeed");

    db.insert_edge_batch([
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
    ])
    .expect("insert_edge_batch should work");

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (config.node_1, config.node_data_1.clone()),
            (config.node_2, config.node_data_2.clone()),
            (config.node_3, config.node_data_3.clone()),
            (config.node_4, config.node_data_4.clone()),
        ]
        .to_vec()
    );
    assert_eq!(
        edges.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_data_1.clone()
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
            )
        ]
        .to_vec()
    );
}
