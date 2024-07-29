use crate::{PropertiesType, TripleStoreQuery};
use crate::{Query, Triple, TripleStoreInsert};

use super::MemTripleStore;

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>
    TripleStoreQuery<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    fn query(
        &self,
        query: Query,
    ) -> Result<MemTripleStore<NodeProperties, EdgeProperties>, Self::Error> {
        Ok(match query {
            Query::NodeProps(nodes) => {
                let mut result = MemTripleStore::new();
                for node in nodes {
                    if let Some(data) = self.node_props.get(&node) {
                        result.node_props.insert(node, data.clone());
                    }
                }
                result
            }

            Query::EdgeProps(triples) => {
                let mut result = MemTripleStore::new();
                for triple in triples.into_iter() {
                    if let Some(data_id) = self.spo_data.get(&triple.encode_spo()) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(triple, data.clone())?;
                        }
                    }
                }
                result
            }

            Query::S(items) => {
                let mut result = MemTripleStore::new();
                for sub in items {
                    for (key, data_id) in self.spo_data.range(Triple::key_bounds_1(sub)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_spo(&key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::SP(items) => {
                let mut result = MemTripleStore::new();
                for (sub, pred) in items {
                    for (key, data_id) in self.spo_data.range(Triple::key_bounds_2(sub, pred)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_spo(&key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::SO(items) => {
                let mut result = MemTripleStore::new();
                for (sub, obj) in items {
                    for (key, data_id) in self.osp_data.range(Triple::key_bounds_2(obj, sub)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_osp(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::P(items) => {
                let mut result = MemTripleStore::new();
                for pred in items {
                    for (key, data_id) in self.pos_data.range(Triple::key_bounds_1(pred)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_pos(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::PO(items) => {
                let mut result = MemTripleStore::new();
                for (pred, obj) in items {
                    for (key, data_id) in self.pos_data.range(Triple::key_bounds_2(pred, obj)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_pos(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::O(items) => {
                let mut result = MemTripleStore::new();
                for obj in items {
                    for (key, data_id) in self.osp_data.range(Triple::key_bounds_1(obj)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_osp(key), data.clone())?;
                        }
                    }
                }
                result
            }
        })
    }
}

#[cfg(test)]
mod test {
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

    fn build_graph(config: Config) -> MemTripleStore<String, String> {
        let mut result = MemTripleStore::new();

        result
            .insert_node_batch(
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

        result
            .insert_edge_batch(
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

        result
    }

    #[test]
    fn test_query_node_props() {
        let config = Config::default();

        let graph = build_graph(config.clone());

        let query = graph
            .query(Query::NodeProps([config.node_1, config.node_2].into()))
            .expect("ok");

        assert_eq!(
            query.iter_node().collect::<HashSet<_>>(),
            [
                Ok((config.node_1, config.node_props_1)),
                Ok((config.node_2, config.node_props_2))
            ]
            .into()
        );
        assert_eq!(query.iter_edge_spo().collect::<Vec<_>>().len(), 0);
    }

    #[test]
    fn test_query_edge_props() {
        let config = Config::default();

        let graph = build_graph(config.clone());

        let query = graph
            .query(Query::EdgeProps(
                [
                    Triple {
                        sub: config.node_1,
                        pred: config.edge_1,
                        obj: config.node_2,
                    },
                    Triple {
                        sub: config.node_2,
                        pred: config.edge_2,
                        obj: config.node_4,
                    },
                ]
                .into(),
            ))
            .expect("ok");

        assert_eq!(
            query.iter_edge_spo().collect::<HashSet<_>>(),
            [
                Ok((
                    Triple {
                        sub: config.node_1,
                        pred: config.edge_1,
                        obj: config.node_2,
                    },
                    config.edge_props_1
                )),
                Ok((
                    Triple {
                        sub: config.node_2,
                        pred: config.edge_2,
                        obj: config.node_4,
                    },
                    config.edge_props_2
                ))
            ]
            .into()
        );
        assert_eq!(query.iter_node().collect::<Vec<_>>().len(), 0);
    }

    #[test]
    fn test_query_s() {
        let config = Config::default();

        let graph = build_graph(config.clone());

        let query = graph
            .query(Query::S([config.node_1, config.node_3].into()))
            .expect("ok");

        assert_eq!(
            query.iter_edge_spo().collect::<HashSet<_>>(),
            [
                Ok((
                    Triple {
                        sub: config.node_1,
                        pred: config.edge_1,
                        obj: config.node_2,
                    },
                    config.edge_props_1
                )),
                Ok((
                    Triple {
                        sub: config.node_3,
                        pred: config.edge_2,
                        obj: config.node_4,
                    },
                    config.edge_props_3
                ))
            ]
            .into()
        );

        assert_eq!(query.iter_node().collect::<Vec<_>>().len(), 0);
    }

    #[test]
    fn test_query_sp() {
        let config = Config::default();

        let graph = build_graph(config.clone());

        let query = graph
            .query(Query::SP([(config.node_0, config.edge_1)].into()))
            .expect("ok");

        assert_eq!(
            query.iter_edge_spo().collect::<HashSet<_>>(),
            [Ok((
                Triple {
                    sub: config.node_0,
                    pred: config.edge_1,
                    obj: config.node_1,
                },
                config.edge_0_1_props
            )),]
            .into()
        );

        assert_eq!(query.iter_node().collect::<Vec<_>>().len(), 0);
    }

    #[test]
    fn test_query_p() {
        let config = Config::default();

        let graph = build_graph(config.clone());

        let query = graph.query(Query::P([config.edge_1].into())).expect("ok");

        assert_eq!(
            query.iter_edge_pos().collect::<HashSet<_>>(),
            [
                Ok((
                    Triple {
                        sub: config.node_0,
                        pred: config.edge_1,
                        obj: config.node_1,
                    },
                    config.edge_0_1_props,
                ),),
                Ok((
                    Triple {
                        sub: config.node_1,
                        pred: config.edge_1,
                        obj: config.node_2,
                    },
                    config.edge_props_1,
                ))
            ]
            .into()
        );

        assert_eq!(query.iter_node().collect::<Vec<_>>().len(), 0);
    }

    #[test]
    fn test_query_po() {
        let config = Config::default();

        let graph = build_graph(config.clone());

        let query = graph
            .query(Query::PO([(config.edge_1, config.node_2)].into()))
            .expect("ok");

        assert_eq!(
            query.iter_edge_pos().collect::<HashSet<_>>(),
            [Ok((
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_props_1,
            ))]
            .into()
        );

        assert_eq!(query.iter_node().collect::<Vec<_>>().len(), 0);
    }

    #[test]
    fn test_query_o() {
        let config = Config::default();

        let graph = build_graph(config.clone());

        let query = graph.query(Query::O([config.node_4].into())).expect("ok");

        assert_eq!(
            query.iter_edge_osp().collect::<HashSet<_>>(),
            [
                Ok((
                    Triple {
                        sub: config.node_2,
                        pred: config.edge_2,
                        obj: config.node_4,
                    },
                    config.edge_props_2,
                ),),
                Ok((
                    Triple {
                        sub: config.node_3,
                        pred: config.edge_2,
                        obj: config.node_4,
                    },
                    config.edge_props_3,
                ))
            ]
            .into()
        );

        assert_eq!(query.iter_node().collect::<Vec<_>>().len(), 0);
    }
}
