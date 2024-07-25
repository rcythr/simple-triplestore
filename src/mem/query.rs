use ulid::Ulid;

use crate::{Query, Triple, TripleStore};

use super::MemTripleStore;

impl<NodeProperties: Clone, EdgeProperties: Clone> MemTripleStore<NodeProperties, EdgeProperties> {
    pub(super) fn handle_query(
        &mut self,
        query: Query,
    ) -> Result<MemTripleStore<NodeProperties, EdgeProperties>, ()> {
        Ok(match query {
            Query::Union { left, right } => {
                let mut left = self.handle_query(*left)?;
                let right = self.handle_query(*right)?;
                left.extend(right);
                left
            }

            Query::Intersection { left, right } => {
                let left = self.handle_query(*left)?;
                let right = self.handle_query(*right)?;

                let mut result = MemTripleStore::new();

                // Intersect nodes
                for (node, data) in left.node_props {
                    if let Some(_) = right.node_props.get(&node) {
                        result.node_props.insert(node, data);
                    }
                }

                let edge_data = left.edge_props;

                // Intersect edges
                let mut left_iter = left.spo_data.into_iter();
                let mut left = left_iter.next();

                let mut right_iter = right.spo_data.into_iter();
                let mut right = right_iter.next();

                while left.is_some() && right.is_some() {
                    let left_key = left.as_ref().unwrap().0;
                    let right_key = right.as_ref().unwrap().0;

                    if left_key < right_key {
                        left = left_iter.next();
                    } else if right_key < left_key {
                        right = right_iter.next();
                    } else {
                        let triple = Triple::decode_spo(&left_key);
                        if result.node_props.contains_key(&triple.sub)
                            && result.node_props.contains_key(&triple.obj)
                        {
                            if let Some(data) = edge_data.get(&left.as_ref().unwrap().1) {
                                result.handle_insert_edge(triple, data.clone())?;
                            }
                        }
                        left = left_iter.next();
                        right = right_iter.next();
                    }
                }

                result
            }

            Query::Difference { left, right } => {
                let left = self.handle_query(*left)?;
                let right = self.handle_query(*right)?;

                let mut result = MemTripleStore::new();

                // Intersect nodes
                result.node_props = left.node_props.clone();
                for (node, _) in right.node_props {
                    if let Some(_) = left.node_props.get(&node) {
                        result.node_props.remove(&node);
                    }
                }

                let edge_data = left.edge_props;

                // Intersect edges
                let mut left_iter = left.spo_data.into_iter();
                let mut left = left_iter.next();

                let mut right_iter = right.spo_data.into_iter();
                let mut right = right_iter.next();

                while left.is_some() && right.is_some() {
                    let left_key = left.as_ref().unwrap().0;
                    let right_key = right.as_ref().unwrap().0;

                    if left_key < right_key {
                        let triple = Triple::decode_spo(&left_key);

                        if let Some(data) = edge_data.get(&left.as_ref().unwrap().1) {
                            result.handle_insert_edge(triple, data.clone())?;
                        }

                        left = left_iter.next();
                    } else if right_key < left_key {
                        right = right_iter.next();
                    } else {
                        left = left_iter.next();
                        right = right_iter.next();
                    }
                }

                while left.is_some() {
                    let triple = Triple::decode_spo(&left.as_ref().unwrap().0);
                    if let Some(data) = edge_data.get(&left.as_ref().unwrap().1) {
                        result.handle_insert_edge(triple, data.clone())?;
                    }
                    left = left_iter.next();
                }

                result
            }

            Query::NodeProperty(nodes) => {
                let mut result = MemTripleStore::new();
                for node in nodes {
                    if let Some(data) = self.node_props.get(&node) {
                        result.node_props.insert(node, data.clone());
                    }
                }
                result
            }

            Query::EdgeProperty(triples) => {
                let mut result = MemTripleStore::new();
                for triple in triples
                    .into_iter()
                    .map(|(sub, pred, obj)| Triple { sub, pred, obj })
                {
                    if let Some(data_id) = self.spo_data.get(&triple.encode_spo()) {
                        if let Some(sub_data) = self.node_props.get(&triple.sub) {
                            result.handle_insert_node(triple.sub, sub_data.clone())?;
                        }

                        if let Some(obj_data) = self.node_props.get(&triple.obj) {
                            result.handle_insert_node(triple.obj, obj_data.clone())?;
                        }

                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.handle_insert_edge(triple, data.clone())?;
                        }
                    }
                }
                result
            }

            Query::S__(items) => {
                let mut result = MemTripleStore::new();
                for item in items {
                    for (key, data_id) in self.spo_data.range((
                        std::ops::Bound::Included(
                            Triple {
                                sub: item,
                                pred: Ulid(u128::MIN),
                                obj: Ulid(u128::MIN),
                            }
                            .encode_spo(),
                        ),
                        std::ops::Bound::Included(
                            Triple {
                                sub: item,
                                pred: Ulid(u128::MAX),
                                obj: Ulid(u128::MAX),
                            }
                            .encode_spo(),
                        ),
                    )) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.handle_insert_edge(Triple::decode_spo(&key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::SP_(items) => {
                let mut result = MemTripleStore::new();
                for item in items {
                    for (key, data_id) in self.spo_data.range((
                        std::ops::Bound::Included(
                            Triple {
                                sub: item.0,
                                pred: item.1,
                                obj: Ulid(u128::MIN),
                            }
                            .encode_spo(),
                        ),
                        std::ops::Bound::Included(
                            Triple {
                                sub: item.0,
                                pred: item.1,
                                obj: Ulid(u128::MAX),
                            }
                            .encode_spo(),
                        ),
                    )) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.handle_insert_edge(Triple::decode_spo(&key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::S_O(items) => {
                let mut result = MemTripleStore::new();
                for item in items {
                    for (key, data_id) in self.osp_data.range((
                        std::ops::Bound::Included(
                            Triple {
                                sub: item.0,
                                pred: Ulid(u128::MIN),
                                obj: item.1,
                            }
                            .encode_osp(),
                        ),
                        std::ops::Bound::Included(
                            Triple {
                                sub: item.0,
                                pred: Ulid(u128::MAX),
                                obj: item.1,
                            }
                            .encode_osp(),
                        ),
                    )) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.handle_insert_edge(Triple::decode_osp(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::_P_(items) => {
                let mut result = MemTripleStore::new();
                for item in items {
                    for (key, data_id) in self.pos_data.range((
                        std::ops::Bound::Included(
                            Triple {
                                sub: Ulid(u128::MIN),
                                pred: item,
                                obj: Ulid(u128::MIN),
                            }
                            .encode_pos(),
                        ),
                        std::ops::Bound::Included(
                            Triple {
                                sub: Ulid(u128::MAX),
                                pred: item,
                                obj: Ulid(u128::MAX),
                            }
                            .encode_pos(),
                        ),
                    )) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.handle_insert_edge(Triple::decode_pos(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::_PO(items) => {
                let mut result = MemTripleStore::new();
                for item in items {
                    for (key, data_id) in self.pos_data.range((
                        std::ops::Bound::Included(
                            Triple {
                                sub: Ulid(u128::MIN),
                                pred: item.0,
                                obj: item.1,
                            }
                            .encode_pos(),
                        ),
                        std::ops::Bound::Included(
                            Triple {
                                sub: Ulid(u128::MAX),
                                pred: item.0,
                                obj: item.1,
                            }
                            .encode_pos(),
                        ),
                    )) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.handle_insert_edge(Triple::decode_pos(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::__O(items) => {
                let mut result = MemTripleStore::new();
                for item in items {
                    for (key, data_id) in self.osp_data.range((
                        std::ops::Bound::Included(
                            Triple {
                                sub: Ulid(u128::MIN),
                                pred: Ulid(u128::MIN),
                                obj: item,
                            }
                            .encode_osp(),
                        ),
                        std::ops::Bound::Included(
                            Triple {
                                sub: Ulid(u128::MAX),
                                pred: Ulid(u128::MAX),
                                obj: item,
                            }
                            .encode_osp(),
                        ),
                    )) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.handle_insert_edge(Triple::decode_osp(key), data.clone())?;
                        }
                    }
                }
                result
            }
        })
    }
}
