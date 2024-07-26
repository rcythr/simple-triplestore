use crate::{Query, Triple, TripleStoreInsert};
use crate::{TripleStoreExtend, TripleStoreQuery};

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
                left.extend(right)?;
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
                                result.insert_edge(triple, data.clone())?;
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
                            result.insert_edge(triple, data.clone())?;
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
                        result.insert_edge(triple, data.clone())?;
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
                            result.insert_node(triple.sub, sub_data.clone())?;
                        }

                        if let Some(obj_data) = self.node_props.get(&triple.obj) {
                            result.insert_node(triple.obj, obj_data.clone())?;
                        }

                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(triple, data.clone())?;
                        }
                    }
                }
                result
            }

            Query::S__(items) => {
                let mut result = MemTripleStore::new();
                for sub in items {
                    for (key, data_id) in self.spo_data.range(Triple::key_bounds_s(sub)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_spo(&key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::SP_(items) => {
                let mut result = MemTripleStore::new();
                for (sub, pred) in items {
                    for (key, data_id) in self.spo_data.range(Triple::key_bounds_sp(sub, pred)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_spo(&key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::S_O(items) => {
                let mut result = MemTripleStore::new();
                for (sub, obj) in items {
                    for (key, data_id) in self.osp_data.range(Triple::key_bounds_os(obj, sub)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_osp(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::_P_(items) => {
                let mut result = MemTripleStore::new();
                for pred in items {
                    for (key, data_id) in self.pos_data.range(Triple::key_bounds_p(pred)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_pos(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::_PO(items) => {
                let mut result = MemTripleStore::new();
                for (pred, obj) in items {
                    for (key, data_id) in self.pos_data.range(Triple::key_bounds_po(pred, obj)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result.insert_edge(Triple::decode_pos(key), data.clone())?;
                        }
                    }
                }
                result
            }

            Query::__O(items) => {
                let mut result = MemTripleStore::new();
                for obj in items {
                    for (key, data_id) in self.osp_data.range(Triple::key_bounds_o(obj)) {
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

impl<NodeProperties: Clone, EdgeProperties: Clone> TripleStoreQuery<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    fn query(
        &mut self,
        query: Query,
    ) -> Result<MemTripleStore<NodeProperties, EdgeProperties>, Self::Error> {
        self.handle_query(query)
    }
}
