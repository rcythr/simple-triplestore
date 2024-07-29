use ulid::Ulid;

use crate::{Mergeable, PropertyType, Triple, TripleStoreMerge};

use super::{MemTripleStore, MergeError, TripleStore};

impl<NodeProperties: PropertyType + Mergeable, EdgeProperties: PropertyType + Mergeable>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    fn merge_edge_create_data(
        &mut self,
        old_edge_data_id: Option<Ulid>,
        new_edge_data: EdgeProperties,
    ) -> Ulid {
        if let Some(old_edge_data_id) = old_edge_data_id {
            match self.edge_props.entry(old_edge_data_id.clone()) {
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(new_edge_data)
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(new_edge_data);
                }
            }
            old_edge_data_id
        } else {
            self.insert_edge_create_data(&old_edge_data_id, new_edge_data)
        }
    }
}

impl<NodeProperties: PropertyType + Mergeable, EdgeProperties: PropertyType + Mergeable>
    TripleStoreMerge<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn merge<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(crate::EdgeOrder::SPO);

        for r in other_nodes {
            let (id, data) = r.map_err(|e| MergeError::Right(e))?;

            match self.node_props.entry(id) {
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(data);
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(data);
                }
            }
        }

        for r in other_edges {
            let (id, other_edge_props) = r.map_err(|e| MergeError::Right(e))?;

            match self.spo_data.entry(id.encode_spo()) {
                std::collections::btree_map::Entry::Vacant(self_spo_data_v) => {
                    // We don't have this edge already.
                    let other_edge_props_id = Ulid::new();

                    self_spo_data_v.insert(other_edge_props_id);
                    self.edge_props
                        .insert(other_edge_props_id, other_edge_props);
                }

                std::collections::btree_map::Entry::Occupied(self_spo_data_o) => {
                    let self_edge_props_id = self_spo_data_o.get();

                    let self_edge_data = self.edge_props.entry(*self_edge_props_id);

                    // Merge our edge props using the existing id.
                    match self_edge_data {
                        std::collections::btree_map::Entry::Vacant(v) => {
                            v.insert(other_edge_props);
                        }

                        std::collections::btree_map::Entry::Occupied(mut self_o) => {
                            self_o.get_mut().merge(other_edge_props)
                        }
                    }
                }
            };
        }

        Ok(())
    }

    fn merge_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), ()> {
        match self.node_props.entry(node) {
            std::collections::btree_map::Entry::Occupied(mut o) => {
                o.get_mut().merge(data);
            }
            std::collections::btree_map::Entry::Vacant(v) => {
                v.insert(data);
            }
        }
        Ok(())
    }

    fn merge_node_batch<I: IntoIterator<Item = (Ulid, NodeProperties)>>(
        &mut self,
        nodes: I,
    ) -> Result<(), ()> {
        for (node, data) in nodes {
            self.merge_node(node, data)?;
        }
        Ok(())
    }

    fn merge_edge(&mut self, triple: Triple, data: EdgeProperties) -> Result<(), ()> {
        let old_edge_data_id = match self.spo_data.entry(Triple::encode_spo(&triple)) {
            std::collections::btree_map::Entry::Vacant(_) => None,
            std::collections::btree_map::Entry::Occupied(o) => Some(o.get().clone()),
        };

        let new_edge_data_id = self.merge_edge_create_data(old_edge_data_id, data);

        self.insert_edge_data_internal(&triple, &new_edge_data_id);

        Ok(())
    }

    fn merge_edge_batch<I: IntoIterator<Item = (Triple, EdgeProperties)>>(
        &mut self,
        triples: I,
    ) -> Result<(), ()> {
        for (triple, data) in triples {
            self.merge_edge(triple, data)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};
    use ulid::Ulid;

    use crate::{MemTripleStore, Mergeable, Triple, TripleStoreInsert, TripleStoreMerge};

    #[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
    struct TestMergeable {
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

    fn build_graph(config: Config) -> MemTripleStore<TestMergeable, TestMergeable> {
        let mut result = MemTripleStore::new();

        result
            .insert_node(config.node_1, config.node_1_props)
            .expect("success");
        result
            .insert_node(config.node_2, config.node_2_props)
            .expect("success");
        result
            .insert_node(config.node_3, config.node_3_props)
            .expect("success");
        result
            .insert_node(config.node_4, config.node_4_props)
            .expect("success");

        result
            .insert_edge(
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_3,
                },
                config.edge_1_props,
            )
            .expect("success");
        result
            .insert_edge(
                Triple {
                    sub: config.node_1,
                    pred: config.edge_2,
                    obj: config.node_3,
                },
                config.edge_2_props,
            )
            .expect("success");
        result
            .insert_edge(
                Triple {
                    sub: config.node_3,
                    pred: config.edge_3,
                    obj: config.node_4,
                },
                config.edge_3_props,
            )
            .expect("success");

        result
    }

    #[test]
    fn test_merge() {
        let initial_graph = Config::default();

        let graph_1 = build_graph(Config {
            node_2_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            edge_3_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        });

        let graph_2 = build_graph(Config {
            node_2_props: TestMergeable {
                a: Some("foo".into()),
                b: None,
            },
            edge_3_props: TestMergeable {
                a: Some("foo".into()),
                b: None,
            },
            ..initial_graph.clone()
        });

        let expected_graph_1_2 = build_graph(Config {
            node_2_props: TestMergeable {
                a: Some("foo".into()),
                b: Some("bar".into()),
            },
            edge_3_props: TestMergeable {
                a: Some("foo".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        });

        let expected_graph_2_1 = build_graph(Config {
            node_2_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            edge_3_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        });

        let mut actual_graph_1_2 = graph_1.clone();
        actual_graph_1_2.merge(graph_2.clone()).expect("ok");

        let mut actual_graph_2_1 = graph_2.clone();
        actual_graph_2_1.merge(graph_1).expect("ok");

        assert_eq!(actual_graph_1_2, expected_graph_1_2);
        assert_eq!(actual_graph_2_1, expected_graph_2_1);
    }

    #[test]
    fn test_merge_node() {
        let initial_graph = Config::default();

        let mut graph_1 = build_graph(Config {
            node_2_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        });

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

        let expected_graph = build_graph(Config {
            node_1_props: TestMergeable {
                a: Some("baz".into()),
                b: None,
            },
            node_2_props: TestMergeable {
                a: Some("baz".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        });

        assert_eq!(graph_1, expected_graph);
    }

    #[test]
    fn test_merge_node_batch() {
        let initial_graph = Config::default();

        let mut graph_1 = build_graph(Config {
            node_2_props: TestMergeable {
                a: Some("bar".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        });

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

        let expected_graph = build_graph(Config {
            node_1_props: TestMergeable {
                a: Some("baz".into()),
                b: None,
            },
            node_2_props: TestMergeable {
                a: Some("baz".into()),
                b: Some("bar".into()),
            },
            ..initial_graph.clone()
        });

        assert_eq!(graph_1, expected_graph);
    }

    #[test]
    fn test_merge_edge() {
        let mut graph = MemTripleStore::new();

        let mut gen = ulid::Generator::new();
        let node_1 = gen.generate().unwrap();
        let node_2 = gen.generate().unwrap();
        let node_3 = gen.generate().unwrap();
        let edge = gen.generate().unwrap();

        // Setup the Initial Graph
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

        // The final graph contains the two new edges with proper merged data.
        let mut expected_graph = graph.clone();
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

        assert_eq!(graph, expected_graph);
    }

    #[test]
    fn test_merge_edge_batch() {
        let mut graph = MemTripleStore::new();

        let mut gen = ulid::Generator::new();
        let node_1 = gen.generate().unwrap();
        let node_2 = gen.generate().unwrap();
        let node_3 = gen.generate().unwrap();
        let edge = gen.generate().unwrap();

        // Setup the Initial Graph
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

        // The final graph contains the two new edges with proper merged data.
        let mut expected_graph = graph.clone();
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

        assert_eq!(graph, expected_graph);
    }
}
