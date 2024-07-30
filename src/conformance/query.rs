use std::collections::HashSet;

use ulid::Ulid;

use crate::prelude::*;

#[derive(Clone)]
struct Config {
    node_0: Ulid,
    node_props_0: String,
    edge_0_1_props: String,
    edge_0_2_props: String,

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
    edge_props_3: String,
    node_4: Ulid,
    node_props_4: String,
}

impl Default for Config {
    fn default() -> Self {
        let mut gen = ulid::Generator::new();

        let node_0 = gen.generate().unwrap();
        let node_1 = gen.generate().unwrap();
        let node_4 = gen.generate().unwrap();
        let node_2 = gen.generate().unwrap();
        let node_3 = gen.generate().unwrap();

        let edge_1 = gen.generate().unwrap();
        let edge_2 = gen.generate().unwrap();

        Self {
            node_0,
            node_props_0: "a".into(),
            edge_0_1_props: "e_a_b".into(),
            edge_0_2_props: "e_a_c".into(),

            node_1,
            node_props_1: "b".into(),
            edge_1,
            edge_props_1: "e_b_c".into(),
            node_2,
            node_props_2: "c".into(),
            edge_2,
            edge_props_2: "e_c_g".into(),
            node_3,
            node_props_3: "e".into(),
            edge_props_3: "e_e_g".into(),
            node_4,
            node_props_4: "g".into(),
        }
    }
}

fn build_graph<T: TripleStore<String, String>>(mut db: T, config: Config) -> T {
    db.insert_node_batch(
        [
            (config.node_0, config.node_props_0),
            (config.node_1, config.node_props_1),
            (config.node_2, config.node_props_2),
            (config.node_3, config.node_props_3),
            (config.node_4, config.node_props_4),
        ]
        .into_iter(),
    )
    .expect("success");

    db.insert_edge_batch(
        [
            (
                Triple {
                    sub: config.node_0,
                    pred: config.edge_1,
                    obj: config.node_1,
                },
                config.edge_0_1_props,
            ),
            (
                Triple {
                    sub: config.node_0,
                    pred: config.edge_2,
                    obj: config.node_2,
                },
                config.edge_0_2_props,
            ),
            (
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_props_1,
            ),
            (
                Triple {
                    sub: config.node_2,
                    pred: config.edge_2,
                    obj: config.node_4,
                },
                config.edge_props_2,
            ),
            (
                Triple {
                    sub: config.node_3,
                    pred: config.edge_2,
                    obj: config.node_4,
                },
                config.edge_props_3,
            ),
        ]
        .into_iter(),
    )
    .expect("success");

    db
}

pub(crate) fn test_query_node_props<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph
        .run(Query::NodeProps([config.node_1, config.node_2].into()))
        .expect("ok");

    assert_eq!(
        query
            .iter_vertices()
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [
            (config.node_1, config.node_props_1),
            (config.node_2, config.node_props_2)
        ]
        .into()
    );
    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::SPO)
            .collect::<Vec<_>>()
            .len(),
        0
    );
}

pub(crate) fn test_query_edge_props<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph
        .run(Query::SPO(
            [
                (config.node_1, config.edge_1, config.node_2),
                (config.node_2, config.edge_2, config.node_4),
            ]
            .into(),
        ))
        .expect("ok");

    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::SPO)
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [
            (
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_2,
                    pred: config.edge_2,
                    obj: config.node_4,
                },
                config.edge_props_2
            )
        ]
        .into()
    );
    assert_eq!(query.iter_vertices().collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_query_s<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph
        .run(Query::S([config.node_1, config.node_3].into()))
        .expect("ok");

    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::SPO)
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [
            (
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_props_1
            ),
            (
                Triple {
                    sub: config.node_3,
                    pred: config.edge_2,
                    obj: config.node_4,
                },
                config.edge_props_3
            )
        ]
        .into()
    );

    assert_eq!(query.iter_vertices().collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_query_sp<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph
        .run(Query::SP([(config.node_0, config.edge_1)].into()))
        .expect("ok");

    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::SPO)
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [(
            Triple {
                sub: config.node_0,
                pred: config.edge_1,
                obj: config.node_1,
            },
            config.edge_0_1_props
        ),]
        .into()
    );

    assert_eq!(query.iter_vertices().collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_query_p<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph.run(Query::P([config.edge_1].into())).expect("ok");

    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::POS)
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [
            (
                Triple {
                    sub: config.node_0,
                    pred: config.edge_1,
                    obj: config.node_1,
                },
                config.edge_0_1_props,
            ),
            (
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_props_1,
            )
        ]
        .into()
    );

    assert_eq!(query.iter_vertices().collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_query_po<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph
        .run(Query::PO([(config.edge_1, config.node_2)].into()))
        .expect("ok");

    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::POS)
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [(
            Triple {
                sub: config.node_1,
                pred: config.edge_1,
                obj: config.node_2,
            },
            config.edge_props_1,
        )]
        .into()
    );

    assert_eq!(query.iter_vertices().collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_query_o<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph.run(Query::O([config.node_4].into())).expect("ok");

    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::OSP)
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [
            (
                Triple {
                    sub: config.node_2,
                    pred: config.edge_2,
                    obj: config.node_4,
                },
                config.edge_props_2,
            ),
            (
                Triple {
                    sub: config.node_3,
                    pred: config.edge_2,
                    obj: config.node_4,
                },
                config.edge_props_3,
            )
        ]
        .into()
    );

    assert_eq!(query.iter_vertices().collect::<Vec<_>>().len(), 0);
}

pub(crate) fn test_query_os<T: TripleStore<String, String>>(db: T) {
    let config = Config::default();

    let graph = build_graph(db, config.clone());

    let query = graph
        .run(Query::SO([(config.node_2, config.node_4)].into()))
        .expect("ok");

    assert_eq!(
        query
            .iter_edges(crate::EdgeOrder::OSP)
            .map(|r| r.expect("ok"))
            .collect::<HashSet<_>>(),
        [(
            Triple {
                sub: config.node_2,
                pred: config.edge_2,
                obj: config.node_4,
            },
            config.edge_props_2,
        )]
        .into()
    );

    assert_eq!(query.iter_vertices().collect::<Vec<_>>().len(), 0);
}
