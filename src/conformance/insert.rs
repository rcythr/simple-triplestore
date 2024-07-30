use ulid::Ulid;

use crate::prelude::*;

pub(crate) fn test_insert_node<T: TripleStore<String, String>>(mut db: T) {
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

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);

    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (node_1, data_1.clone()),
            (node_2, data_2.clone()),
            (node_3, data_3.clone()),
            (node_4, data_4.clone()),
        ]
        .to_vec()
    );
    assert_eq!(edges.collect::<Vec<_>>().len(), 0);

    // Update one of the entries by replacement.
    db.insert_node(node_4, data_5.clone())
        .expect("Insert should succeed");

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (node_1, data_1),
            (node_2, data_2),
            (node_3, data_3),
            (node_4, data_5),
        ]
        .to_vec()
    );
    assert_eq!(edges.collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_insert_node_batch<T: TripleStore<String, String>>(mut db: T) {
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

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (node_1, data_1.clone()),
            (node_2, data_2.clone()),
            (node_3, data_3.clone()),
            (node_4, data_5.clone()),
        ]
        .to_vec()
    );
    assert_eq!(edges.collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_insert_edge<T: TripleStore<String, String>>(mut db: T) {
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

    let (edge_1, edge_data_1) = (Ulid(10), "-1->".to_string());
    let (edge_2, edge_data_2) = (Ulid(11), "-2->".to_string());
    let (edge_3, edge_data_3) = (Ulid(12), "-3->".to_string());
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

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (node_1, node_data_1.clone()),
            (node_2, node_data_2.clone()),
            (node_3, node_data_3.clone()),
            (node_4, node_data_4.clone()),
        ]
        .to_vec()
    );
    assert_eq!(
        edges.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: node_1,
                    pred: edge_1,
                    obj: node_2,
                },
                edge_data_1.clone()
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
                edge_data_4.clone(),
            )
        ]
        .to_vec()
    );
}

pub(crate) fn test_insert_edge_batch<T: TripleStore<String, String>>(mut db: T) {
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

    let (nodes, edges) = db.iter_nodes(EdgeOrder::SPO);
    assert_eq!(
        nodes.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (node_1, node_data_1.clone()),
            (node_2, node_data_2.clone()),
            (node_3, node_data_3.clone()),
            (node_4, node_data_4.clone()),
        ]
        .to_vec()
    );
    assert_eq!(
        edges.map(|e| e.expect("ok")).collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: node_1,
                    pred: edge_1,
                    obj: node_2,
                },
                edge_data_1.clone()
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
            )
        ]
        .to_vec()
    );
}
