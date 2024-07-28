use ulid::Ulid;

use crate::{Mergeable, PropertiesType, Triple, TripleStoreMerge};

use super::MemTripleStore;

impl<NodeProperties: PropertiesType + Mergeable, EdgeProperties: PropertiesType + Mergeable>
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

impl<NodeProperties: PropertiesType + Mergeable, EdgeProperties: PropertiesType + Mergeable>
    TripleStoreMerge<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn merge(&mut self, mut other: Self) {
        for (id, data) in other.node_props {
            match self.node_props.entry(id) {
                std::collections::btree_map::Entry::Occupied(mut o) => {
                    o.get_mut().merge(data);
                }
                std::collections::btree_map::Entry::Vacant(v) => {
                    v.insert(data);
                }
            }
        }

        for (id, other_edge_props_id) in other.spo_data {
            match self.spo_data.entry(id) {
                std::collections::btree_map::Entry::Vacant(self_spo_data_v) => {
                    // We don't have this edge already.
                    // Get the content from other.edge_props
                    other
                        .edge_props
                        .remove(&other_edge_props_id)
                        .map(|other_edge_props| {
                            self_spo_data_v.insert(other_edge_props_id);
                            self.edge_props
                                .insert(other_edge_props_id, other_edge_props);
                        });
                }

                std::collections::btree_map::Entry::Occupied(self_spo_data_o) => {
                    let self_edge_props_id = self_spo_data_o.get();

                    let self_edge_data = self.edge_props.entry(*self_edge_props_id);
                    let other_edge_data = other.edge_props.entry(other_edge_props_id);

                    // Merge our edge props using the existing id.

                    match (self_edge_data, other_edge_data) {
                        (
                            std::collections::btree_map::Entry::Vacant(_),
                            std::collections::btree_map::Entry::Vacant(_),
                        ) => {}
                        (
                            std::collections::btree_map::Entry::Vacant(v),
                            std::collections::btree_map::Entry::Occupied(o),
                        ) => {
                            v.insert(o.remove());
                        }
                        (
                            std::collections::btree_map::Entry::Occupied(_),
                            std::collections::btree_map::Entry::Vacant(_),
                        ) => {
                            // Nothing to do.
                        }
                        (
                            std::collections::btree_map::Entry::Occupied(mut self_o),
                            std::collections::btree_map::Entry::Occupied(other_o),
                        ) => self_o.get_mut().merge(other_o.remove()),
                    }
                }
            };
        }
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

    fn merge_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (Ulid, NodeProperties)>,
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

    fn merge_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (Triple, EdgeProperties)>,
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

        dbg!(graph_1.clone());

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

        dbg!(graph_2.clone());

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
        actual_graph_1_2.merge(graph_2.clone());

        let mut actual_graph_2_1 = graph_2.clone();
        actual_graph_2_1.merge(graph_1);

        assert_eq!(actual_graph_1_2, expected_graph_1_2);
        assert_eq!(actual_graph_2_1, expected_graph_2_1);
    }

    #[test]
    fn test_merge_node() {}

    #[test]
    fn test_merge_node_batch() {}

    #[test]
    fn test_merge_edge() {}

    #[test]
    fn test_merge_edge_batch() {}
}
