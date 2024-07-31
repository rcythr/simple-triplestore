use ulid::Ulid;

use crate::{prelude::*, PropsTriple, Triple};

#[derive(Clone)]
struct Config {
    node_1: Ulid,
    node_props_1: String,
    edge_1: Ulid,
    edge_props_1: String,
    node_2: Ulid,
    node_props_2: String,
    edge_2: Ulid,
    edge_props_2: String,
    node_3: Ulid,
    node_props_3: String,
    edge_3: Ulid,
    edge_props_3: String,
    node_4: Ulid,
    node_props_4: String,
}

impl Default for Config {
    fn default() -> Self {
        let mut gen = ulid::Generator::new();

        let node_1 = gen.generate().unwrap();
        let node_4 = gen.generate().unwrap();
        let node_2 = gen.generate().unwrap();
        let node_3 = gen.generate().unwrap();

        let edge_1 = gen.generate().unwrap();
        let edge_3 = gen.generate().unwrap();
        let edge_2 = gen.generate().unwrap();

        Self {
            node_1,
            node_props_1: "a".to_string(),
            edge_1,
            edge_props_1: "b".to_string(),
            node_2,
            node_props_2: "c".to_string(),
            edge_2,
            edge_props_2: "d".to_string(),
            node_3,
            node_props_3: "e".to_string(),
            edge_3,
            edge_props_3: "f".to_string(),
            node_4,
            node_props_4: "g".to_string(),
        }
    }
}

fn build_graph<T: TripleStore<Ulid, String, String>>(db: &mut T, config: Config) {
    db.insert_node(config.node_1, config.node_props_1)
        .expect("success");
    db.insert_node(config.node_2, config.node_props_2)
        .expect("success");
    db.insert_node(config.node_3, config.node_props_3)
        .expect("success");
    db.insert_node(config.node_4, config.node_props_4)
        .expect("success");

    db.insert_edge(
        Triple {
            sub: config.node_1,
            pred: config.edge_1,
            obj: config.node_2,
        },
        config.edge_props_1,
    )
    .expect("success");

    db.insert_edge(
        Triple {
            sub: config.node_2,
            pred: config.edge_2,
            obj: config.node_3,
        },
        config.edge_props_2,
    )
    .expect("success");

    db.insert_edge(
        Triple {
            sub: config.node_3,
            pred: config.edge_3,
            obj: config.node_4,
        },
        config.edge_props_3,
    )
    .expect("success");
}

pub(crate) fn test_iter_spo<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.iter_edges_with_props(crate::EdgeOrder::SPO)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            PropsTriple {
                sub: (config.node_1, config.node_props_1.clone()),
                pred: (config.edge_1, config.edge_props_1.clone()),
                obj: (config.node_2, config.node_props_2.clone())
            },
            PropsTriple {
                sub: (config.node_2, config.node_props_2.clone()),
                pred: (config.edge_2, config.edge_props_2.clone()),
                obj: (config.node_3, config.node_props_3.clone())
            },
            PropsTriple {
                sub: (config.node_3, config.node_props_3.clone()),
                pred: (config.edge_3, config.edge_props_3.clone()),
                obj: (config.node_4, config.node_props_4.clone())
            },
        ]
        .to_vec()
    );
}

pub(crate) fn test_iter_pos<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.iter_edges_with_props(crate::EdgeOrder::POS)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            PropsTriple {
                sub: (config.node_1, config.node_props_1.clone()),
                pred: (config.edge_1, config.edge_props_1.clone()),
                obj: (config.node_2, config.node_props_2.clone())
            },
            PropsTriple {
                sub: (config.node_3, config.node_props_3.clone()),
                pred: (config.edge_3, config.edge_props_3.clone()),
                obj: (config.node_4, config.node_props_4.clone())
            },
            PropsTriple {
                sub: (config.node_2, config.node_props_2.clone()),
                pred: (config.edge_2, config.edge_props_2.clone()),
                obj: (config.node_3, config.node_props_3.clone())
            },
        ]
        .to_vec()
    );
}

pub(crate) fn test_iter_osp<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.iter_edges_with_props(crate::EdgeOrder::OSP)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            PropsTriple {
                sub: (config.node_3, config.node_props_3.clone()),
                pred: (config.edge_3, config.edge_props_3.clone()),
                obj: (config.node_4, config.node_props_4.clone())
            },
            PropsTriple {
                sub: (config.node_1, config.node_props_1.clone()),
                pred: (config.edge_1, config.edge_props_1.clone()),
                obj: (config.node_2, config.node_props_2.clone())
            },
            PropsTriple {
                sub: (config.node_2, config.node_props_2.clone()),
                pred: (config.edge_2, config.edge_props_2.clone()),
                obj: (config.node_3, config.node_props_3.clone())
            },
        ]
        .to_vec()
    );
}

pub(crate) fn test_iter_edge_spo<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.iter_edges(crate::EdgeOrder::SPO)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_1.clone(),
                    pred: config.edge_1.clone(),
                    obj: config.node_2.clone(),
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_2.clone(),
                    pred: config.edge_2.clone(),
                    obj: config.node_3.clone(),
                },
                config.edge_props_2
            ),
            (
                Triple {
                    sub: config.node_3.clone(),
                    pred: config.edge_3.clone(),
                    obj: config.node_4.clone(),
                },
                config.edge_props_3
            )
        ]
        .to_vec()
    );
}

pub(crate) fn test_iter_edge_pos<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.iter_edges(crate::EdgeOrder::POS)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_1.clone(),
                    pred: config.edge_1.clone(),
                    obj: config.node_2.clone(),
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_3.clone(),
                    pred: config.edge_3.clone(),
                    obj: config.node_4.clone(),
                },
                config.edge_props_3
            ),
            (
                Triple {
                    sub: config.node_2.clone(),
                    pred: config.edge_2.clone(),
                    obj: config.node_3.clone(),
                },
                config.edge_props_2
            ),
        ]
        .to_vec()
    );
}

pub(crate) fn test_iter_edge_osp<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.iter_edges(crate::EdgeOrder::OSP)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_3.clone(),
                    pred: config.edge_3.clone(),
                    obj: config.node_4.clone(),
                },
                config.edge_props_3
            ),
            (
                Triple {
                    sub: config.node_1.clone(),
                    pred: config.edge_1.clone(),
                    obj: config.node_2.clone(),
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_2.clone(),
                    pred: config.edge_2.clone(),
                    obj: config.node_3.clone(),
                },
                config.edge_props_2
            ),
        ]
        .to_vec()
    );
}

pub(crate) fn test_iter_node<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.iter_vertices().map(|r| r.unwrap()).collect::<Vec<_>>(),
        [
            (config.node_1, config.node_props_1),
            (config.node_4, config.node_props_4),
            (config.node_2, config.node_props_2),
            (config.node_3, config.node_props_3),
        ]
        .to_vec()
    );
}

pub(crate) fn test_into_iter_spo<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.into_iter_edges_with_props(crate::EdgeOrder::SPO)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            PropsTriple {
                sub: (config.node_1, config.node_props_1.clone()),
                pred: (config.edge_1, config.edge_props_1.clone()),
                obj: (config.node_2, config.node_props_2.clone())
            },
            PropsTriple {
                sub: (config.node_2, config.node_props_2.clone()),
                pred: (config.edge_2, config.edge_props_2.clone()),
                obj: (config.node_3, config.node_props_3.clone())
            },
            PropsTriple {
                sub: (config.node_3, config.node_props_3.clone()),
                pred: (config.edge_3, config.edge_props_3.clone()),
                obj: (config.node_4, config.node_props_4.clone())
            },
        ]
        .to_vec()
    );
}

pub(crate) fn test_into_iter_pos<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.into_iter_edges_with_props(crate::EdgeOrder::POS)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            PropsTriple {
                sub: (config.node_1, config.node_props_1.clone()),
                pred: (config.edge_1, config.edge_props_1.clone()),
                obj: (config.node_2, config.node_props_2.clone())
            },
            PropsTriple {
                sub: (config.node_3, config.node_props_3.clone()),
                pred: (config.edge_3, config.edge_props_3.clone()),
                obj: (config.node_4, config.node_props_4.clone())
            },
            PropsTriple {
                sub: (config.node_2, config.node_props_2.clone()),
                pred: (config.edge_2, config.edge_props_2.clone()),
                obj: (config.node_3, config.node_props_3.clone())
            },
        ]
        .to_vec()
    );
}

pub(crate) fn test_into_iter_osp<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.into_iter_edges_with_props(crate::EdgeOrder::OSP)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            PropsTriple {
                sub: (config.node_3, config.node_props_3.clone()),
                pred: (config.edge_3, config.edge_props_3.clone()),
                obj: (config.node_4, config.node_props_4.clone())
            },
            PropsTriple {
                sub: (config.node_1, config.node_props_1.clone()),
                pred: (config.edge_1, config.edge_props_1.clone()),
                obj: (config.node_2, config.node_props_2.clone())
            },
            PropsTriple {
                sub: (config.node_2, config.node_props_2.clone()),
                pred: (config.edge_2, config.edge_props_2.clone()),
                obj: (config.node_3, config.node_props_3.clone())
            },
        ]
        .to_vec()
    );
}

pub(crate) fn test_into_iter_edge_spo<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.into_iter_edges(crate::EdgeOrder::SPO)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_1.clone(),
                    pred: config.edge_1.clone(),
                    obj: config.node_2.clone(),
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_2.clone(),
                    pred: config.edge_2.clone(),
                    obj: config.node_3.clone(),
                },
                config.edge_props_2
            ),
            (
                Triple {
                    sub: config.node_3.clone(),
                    pred: config.edge_3.clone(),
                    obj: config.node_4.clone(),
                },
                config.edge_props_3
            )
        ]
        .to_vec()
    );
}

pub(crate) fn test_into_iter_edge_pos<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.into_iter_edges(crate::EdgeOrder::POS)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_1.clone(),
                    pred: config.edge_1.clone(),
                    obj: config.node_2.clone(),
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_3.clone(),
                    pred: config.edge_3.clone(),
                    obj: config.node_4.clone(),
                },
                config.edge_props_3
            ),
            (
                Triple {
                    sub: config.node_2.clone(),
                    pred: config.edge_2.clone(),
                    obj: config.node_3.clone(),
                },
                config.edge_props_2
            ),
        ]
        .to_vec()
    );
}

pub(crate) fn test_into_iter_edge_osp<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.into_iter_edges(crate::EdgeOrder::OSP)
            .map(|r| r.expect("success"))
            .collect::<Vec<_>>(),
        [
            (
                Triple {
                    sub: config.node_3.clone(),
                    pred: config.edge_3.clone(),
                    obj: config.node_4.clone(),
                },
                config.edge_props_3
            ),
            (
                Triple {
                    sub: config.node_1.clone(),
                    pred: config.edge_1.clone(),
                    obj: config.node_2.clone(),
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_2.clone(),
                    pred: config.edge_2.clone(),
                    obj: config.node_3.clone(),
                },
                config.edge_props_2
            ),
        ]
        .to_vec()
    );
}

pub(crate) fn test_into_iter_node<T: TripleStore<Ulid, String, String>>(mut db: T) {
    let config = Config::default();

    build_graph(&mut db, config.clone());
    assert_eq!(
        db.into_iter_vertices()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>(),
        [
            (config.node_1, config.node_props_1),
            (config.node_4, config.node_props_4),
            (config.node_2, config.node_props_2),
            (config.node_3, config.node_props_3),
        ]
        .to_vec()
    );
}
