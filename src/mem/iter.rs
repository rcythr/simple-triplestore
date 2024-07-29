use std::collections::BTreeMap;

use ulid::Ulid;

use crate::{PropertiesType, PropsTriple, Triple, TripleStoreIntoIter, TripleStoreIter};

use super::MemTripleStore;

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    fn iter_impl(
        node_props: &BTreeMap<Ulid, NodeProperties>,
        edge_props: &BTreeMap<Ulid, EdgeProperties>,
        triple: Triple,
        v: &Ulid,
    ) -> Option<Result<PropsTriple<NodeProperties, EdgeProperties>, ()>> {
        let sub_data = node_props.get(&triple.sub).cloned();
        let pred_data = edge_props.get(v).cloned();
        let obj_data = node_props.get(&triple.obj).cloned();

        match (sub_data, pred_data, obj_data) {
            (Some(sub_props), Some(prod_props), Some(obj_props)) => Some(Ok(PropsTriple {
                sub: (triple.sub, sub_props),
                pred: (triple.pred, prod_props),
                obj: (triple.obj, obj_props),
            })),
            _ => None,
        }
    }
}

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>
    TripleStoreIter<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn iter_spo<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, ()>> + 'a {
        self.spo_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_spo(&k);
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, triple, &v)
        })
    }

    fn iter_pos<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, ()>> + 'a {
        self.pos_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_pos(&k);
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, triple, &v)
        })
    }

    fn iter_osp<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, ()>> + 'a {
        self.osp_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_osp(&k);
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, triple, &v)
        })
    }

    fn iter_edge_spo<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), ()>> + 'a {
        self.spo_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_spo(&k), v.clone()))),
                None => None,
            })
    }

    fn iter_edge_pos<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), ()>> + 'a {
        self.pos_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_pos(&k), v.clone()))),
                None => None,
            })
    }

    fn iter_edge_osp<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), ()>> + 'a {
        self.osp_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_osp(&k), v.clone()))),
                None => None,
            })
    }

    fn iter_node<'a>(&'a self) -> impl Iterator<Item = Result<(Ulid, NodeProperties), ()>> + 'a {
        self.node_props
            .iter()
            .map(|(id, props)| Ok((id.clone(), props.clone())))
    }
}

impl<NodeProperties: PropertiesType + PartialEq, EdgeProperties: PropertiesType + PartialEq>
    TripleStoreIntoIter<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn into_iters(
        self,
    ) -> (
        impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>,
        impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>,
    ) {
        let node_iter = self.node_props.into_iter().map(|o| Ok(o));
        let edge_iter =
            self.spo_data
                .into_iter()
                .filter_map(move |(k, v)| match self.edge_props.get(&v) {
                    Some(v) => Some(Ok((Triple::decode_spo(&k), v.clone()))),
                    None => None,
                });
        (node_iter, edge_iter)
    }

    fn into_iter_spo(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, ()>> {
        self.spo_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_spo(&k);
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, triple, &v)
        })
    }

    fn into_iter_pos(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, ()>> {
        self.pos_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_pos(&k);
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, triple, &v)
        })
    }

    fn into_iter_osp(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, ()>> {
        self.osp_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_osp(&k);
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, triple, &v)
        })
    }

    fn into_iter_node(self) -> impl Iterator<Item = Result<(Ulid, NodeProperties), ()>> {
        self.node_props.into_iter().map(|o| Ok(o))
    }

    fn into_iter_edge_spo(self) -> impl Iterator<Item = Result<(Triple, EdgeProperties), ()>> {
        self.spo_data
            .into_iter()
            .filter_map(move |(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_spo(&k), v.clone()))),
                None => None,
            })
    }

    fn into_iter_edge_pos(self) -> impl Iterator<Item = Result<(Triple, EdgeProperties), ()>> {
        self.pos_data
            .into_iter()
            .filter_map(move |(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_pos(&k), v.clone()))),
                None => None,
            })
    }

    fn into_iter_edge_osp(self) -> impl Iterator<Item = Result<(Triple, EdgeProperties), ()>> {
        self.osp_data
            .into_iter()
            .filter_map(move |(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_osp(&k), v.clone()))),
                None => None,
            })
    }
}

#[cfg(test)]
mod test {
    use ulid::Ulid;

    use crate::{
        MemTripleStore, PropsTriple, Triple, TripleStoreInsert, TripleStoreIntoIter,
        TripleStoreIter,
    };

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

    fn build_graph(config: Config) -> MemTripleStore<String, String> {
        let mut result = MemTripleStore::new();

        result
            .insert_node(config.node_1, config.node_props_1)
            .expect("success");
        result
            .insert_node(config.node_2, config.node_props_2)
            .expect("success");
        result
            .insert_node(config.node_3, config.node_props_3)
            .expect("success");
        result
            .insert_node(config.node_4, config.node_props_4)
            .expect("success");

        result
            .insert_edge(
                Triple {
                    sub: config.node_1,
                    pred: config.edge_1,
                    obj: config.node_2,
                },
                config.edge_props_1,
            )
            .expect("success");

        result
            .insert_edge(
                Triple {
                    sub: config.node_2,
                    pred: config.edge_2,
                    obj: config.node_3,
                },
                config.edge_props_2,
            )
            .expect("success");

        result
            .insert_edge(
                Triple {
                    sub: config.node_3,
                    pred: config.edge_3,
                    obj: config.node_4,
                },
                config.edge_props_3,
            )
            .expect("success");

        result
    }

    #[test]
    fn test_iter_spo() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .iter_spo()
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

    #[test]
    fn test_iter_pos() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .iter_pos()
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

    #[test]
    fn test_iter_osp() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .iter_osp()
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

    #[test]
    fn test_iter_edge_spo() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .iter_edge_spo()
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

    #[test]
    fn test_iter_edge_pos() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .iter_edge_pos()
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

    #[test]
    fn test_iter_edge_osp() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .iter_edge_osp()
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

    #[test]
    fn test_iter_node() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_node()
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

    #[test]
    fn test_into_iter_spo() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_spo()
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

    #[test]
    fn test_into_iter_pos() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_pos()
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

    #[test]
    fn test_into_iter_osp() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_osp()
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

    #[test]
    fn test_into_iter_edge_spo() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_edge_spo()
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

    #[test]
    fn test_into_iter_edge_pos() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_edge_pos()
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

    #[test]
    fn test_into_iter_edge_osp() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_edge_osp()
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

    #[test]
    fn test_into_iter_node() {
        let config = Config::default();

        let graph = build_graph(config.clone());
        assert_eq!(
            graph
                .into_iter_node()
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
}
