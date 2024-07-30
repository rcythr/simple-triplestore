use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::prelude::*;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct TestMergeable {
    a: Option<String>,
    b: Option<String>,
}

impl Default for TestMergeable {
    fn default() -> Self {
        TestMergeable { a: None, b: None }
    }
}

impl Mergeable for TestMergeable {
    fn merge(&mut self, other: Self) {
        other.a.map(|a| {
            self.a = Some(a);
        });
        other.b.map(|b| {
            self.b = Some(b);
        });
    }
}

#[derive(Clone)]
struct Config {
    node_1: Ulid,
    node_1_props: TestMergeable,
    node_2: Ulid,
    node_2_props: TestMergeable,
    node_3: Ulid,
    node_3_props: TestMergeable,
    node_4: Ulid,
    node_4_props: TestMergeable,

    edge_1: Ulid,
    edge_1_props: TestMergeable,
    edge_2: Ulid,
    edge_2_props: TestMergeable,
    edge_3: Ulid,
    edge_3_props: TestMergeable,
}

impl Default for Config {
    fn default() -> Self {
        let mut gen = ulid::Generator::new();
        Self {
            node_1: gen.generate().unwrap(),
            node_1_props: TestMergeable::default(),
            node_2: gen.generate().unwrap(),
            node_2_props: TestMergeable::default(),
            node_3: gen.generate().unwrap(),
            node_3_props: TestMergeable::default(),
            node_4: gen.generate().unwrap(),
            node_4_props: TestMergeable::default(),

            edge_1: gen.generate().unwrap(),
            edge_1_props: TestMergeable::default(),
            edge_2: gen.generate().unwrap(),
            edge_2_props: TestMergeable::default(),
            edge_3: gen.generate().unwrap(),
            edge_3_props: TestMergeable::default(),
        }
    }
}

fn build_graph<
    T: TripleStore<TestMergeable, TestMergeable> + TripleStoreMerge<TestMergeable, TestMergeable>,
>(
    mut db: T,
    config: Config,
) -> T {
    db.insert_node(config.node_1, config.node_1_props)
        .expect("success");
    db.insert_node(config.node_2, config.node_2_props)
        .expect("success");
    db.insert_node(config.node_3, config.node_3_props)
        .expect("success");
    db.insert_node(config.node_4, config.node_4_props)
        .expect("success");

    db.insert_edge(
        Triple {
            sub: config.node_1,
            pred: config.edge_1,
            obj: config.node_3,
        },
        config.edge_1_props,
    )
    .expect("success");
    db.insert_edge(
        Triple {
            sub: config.node_1,
            pred: config.edge_2,
            obj: config.node_3,
        },
        config.edge_2_props,
    )
    .expect("success");
    db.insert_edge(
        Triple {
            sub: config.node_3,
            pred: config.edge_3,
            obj: config.node_4,
        },
        config.edge_3_props,
    )
    .expect("success");

    db
}

pub(crate) fn test_merge<
    T: TripleStore<TestMergeable, TestMergeable>
        + TripleStoreMerge<TestMergeable, TestMergeable>
        + std::fmt::Debug,
>(
    mut make_db: impl FnMut() -> T,
) {
    let initial_graph = Config::default();

    let make_graph_1 = |db| {
        build_graph(
            db,
            Config {
                node_2_props: TestMergeable {
                    a: Some("bar".into()),
                    b: Some("bar".into()),
                },
                edge_3_props: TestMergeable {
                    a: Some("bar".into()),
                    b: Some("bar".into()),
                },
                ..initial_graph.clone()
            },
        )
    };

    let make_graph_2 = |db| {
        build_graph(
            db,
            Config {
                node_2_props: TestMergeable {
                    a: Some("foo".into()),
                    b: None,
                },
                edge_3_props: TestMergeable {
                    a: Some("foo".into()),
                    b: None,
                },
                ..initial_graph.clone()
            },
        )
    };

    let graph_1 = make_graph_1(make_db());
    let graph_2 = make_graph_2(make_db());

    let expected_graph_1_2 = build_graph(
        make_db(),
        Config {
            node_2_props: TestMergeable {
                a: Some("foo".into()),
                b: Some("bar".into()),
            },
            edge_3_props: TestMergeable {
                a: Some("foo".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        },
    );

    let expected_graph_2_1 = build_graph(
        make_db(),
        Config {
            node_2_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            edge_3_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        },
    );

    let mut actual_graph_1_2 = graph_1;
    actual_graph_1_2.merge(make_graph_2(make_db())).expect("ok");

    let mut actual_graph_2_1 = graph_2;
    actual_graph_2_1.merge(make_graph_1(make_db())).expect("ok");

    assert!(actual_graph_1_2.try_eq(&expected_graph_1_2).expect("ok"));
    assert!(actual_graph_2_1.try_eq(&expected_graph_2_1).expect("ok"));
}

pub(crate) fn test_merge_node<
    T: TripleStore<TestMergeable, TestMergeable>
        + TripleStoreMerge<TestMergeable, TestMergeable>
        + std::fmt::Debug,
>(
    mut make_db: impl FnMut() -> T,
) {
    let initial_graph = Config::default();

    let mut graph_1 = build_graph(
        make_db(),
        Config {
            node_2_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        },
    );

    graph_1
        .merge_node(
            initial_graph.node_1,
            TestMergeable {
                a: Some("baz".into()),
                b: None,
            },
        )
        .expect("ok");

    graph_1
        .merge_node(
            initial_graph.node_2,
            TestMergeable {
                a: Some("baz".into()),
                b: None,
            },
        )
        .expect("ok");

    let expected_graph = build_graph(
        make_db(),
        Config {
            node_1_props: TestMergeable {
                a: Some("baz".into()),
                b: None,
            },
            node_2_props: TestMergeable {
                a: Some("baz".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        },
    );

    assert!(graph_1.try_eq(&expected_graph).expect("ok"));
}

pub(crate) fn test_merge_node_batch<
    T: TripleStore<TestMergeable, TestMergeable>
        + TripleStoreMerge<TestMergeable, TestMergeable>
        + std::fmt::Debug,
>(
    mut make_db: impl FnMut() -> T,
) {
    let initial_graph = Config::default();

    let mut graph_1 = build_graph(
        make_db(),
        Config {
            node_2_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        },
    );

    graph_1
        .merge_node_batch(
            [
                (
                    initial_graph.node_1,
                    TestMergeable {
                        a: Some("baz".into()),
                        b: None,
                    },
                ),
                (
                    initial_graph.node_2,
                    TestMergeable {
                        a: Some("baz".into()),
                        b: None,
                    },
                ),
            ]
            .into_iter(),
        )
        .expect("ok");

    let expected_graph = build_graph(
        make_db(),
        Config {
            node_1_props: TestMergeable {
                a: Some("baz".into()),
                b: None,
            },
            node_2_props: TestMergeable {
                a: Some("baz".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        },
    );

    assert!(graph_1.try_eq(&expected_graph).expect("ok"));
}

pub(crate) fn test_merge_edge<
    T: TripleStore<TestMergeable, TestMergeable>
        + TripleStoreMerge<TestMergeable, TestMergeable>
        + std::fmt::Debug,
>(
    mut make_db: impl FnMut() -> T,
) {
    let mut gen = ulid::Generator::new();
    let node_1 = gen.generate().unwrap();
    let node_2 = gen.generate().unwrap();
    let node_3 = gen.generate().unwrap();
    let edge = gen.generate().unwrap();

    // Setup the Initial Graph
    let make_graph = |mut graph: T| {
        graph
            .insert_node(node_1, TestMergeable::default())
            .expect("ok");
        graph
            .insert_node(node_2, TestMergeable::default())
            .expect("ok");
        graph
            .insert_node(node_3, TestMergeable::default())
            .expect("ok");
        graph
            .insert_edge(
                Triple {
                    sub: node_1,
                    pred: edge,
                    obj: node_2,
                },
                TestMergeable {
                    a: Some("foo".into()),
                    b: None,
                },
            )
            .expect("ok");
        graph
    };

    let mut graph = make_graph(make_db());

    // The final graph contains the two new edges with proper merged data.
    let mut expected_graph = make_graph(make_db());
    expected_graph
        .insert_edge(
            Triple {
                sub: node_1,
                pred: edge,
                obj: node_2,
            },
            TestMergeable {
                a: Some("foo".into()),
                b: Some("bar".into()),
            },
        )
        .expect("ok");
    expected_graph
        .insert_edge(
            Triple {
                sub: node_2,
                pred: edge,
                obj: node_3,
            },
            TestMergeable {
                a: Some("baz".into()),
                b: Some("baz".into()),
            },
        )
        .expect("ok");

    // Perform the merge_edge
    graph
        .merge_edge(
            Triple {
                sub: node_1,
                pred: edge,
                obj: node_2,
            },
            TestMergeable {
                a: None,
                b: Some("bar".into()),
            },
        )
        .expect("ok");
    graph
        .merge_edge(
            Triple {
                sub: node_2,
                pred: edge,
                obj: node_3,
            },
            TestMergeable {
                a: Some("baz".into()),
                b: Some("baz".into()),
            },
        )
        .expect("ok");

    assert!(graph.try_eq(&expected_graph).expect("ok"));
}

pub(crate) fn test_merge_edge_batch<
    T: TripleStore<TestMergeable, TestMergeable>
        + TripleStoreMerge<TestMergeable, TestMergeable>
        + std::fmt::Debug,
>(
    mut make_db: impl FnMut() -> T,
) {
    let mut gen = ulid::Generator::new();
    let node_1 = gen.generate().unwrap();
    let node_2 = gen.generate().unwrap();
    let node_3 = gen.generate().unwrap();
    let edge = gen.generate().unwrap();

    // Setup the Initial Graph
    let make_graph = |mut graph: T| {
        graph
            .insert_node(node_1, TestMergeable::default())
            .expect("ok");
        graph
            .insert_node(node_2, TestMergeable::default())
            .expect("ok");
        graph
            .insert_node(node_3, TestMergeable::default())
            .expect("ok");
        graph
            .insert_edge(
                Triple {
                    sub: node_1,
                    pred: edge,
                    obj: node_2,
                },
                TestMergeable {
                    a: Some("foo".into()),
                    b: None,
                },
            )
            .expect("ok");
        graph
    };

    // The final graph contains the two new edges with proper merged data.
    let mut expected_graph = make_graph(make_db());
    expected_graph
        .insert_edge(
            Triple {
                sub: node_1,
                pred: edge,
                obj: node_2,
            },
            TestMergeable {
                a: Some("foo".into()),
                b: Some("bar".into()),
            },
        )
        .expect("ok");
    expected_graph
        .insert_edge(
            Triple {
                sub: node_2,
                pred: edge,
                obj: node_3,
            },
            TestMergeable {
                a: Some("baz".into()),
                b: Some("baz".into()),
            },
        )
        .expect("ok");

    // Perform the merge_edge
    let mut graph = make_graph(make_db());
    graph
        .merge_edge_batch(
            [
                (
                    Triple {
                        sub: node_1,
                        pred: edge,
                        obj: node_2,
                    },
                    TestMergeable {
                        a: None,
                        b: Some("bar".into()),
                    },
                ),
                (
                    Triple {
                        sub: node_2,
                        pred: edge,
                        obj: node_3,
                    },
                    TestMergeable {
                        a: Some("baz".into()),
                        b: Some("baz".into()),
                    },
                ),
            ]
            .into_iter(),
        )
        .expect("ok");

    assert!(graph.try_eq(&expected_graph).expect("ok"));
}
