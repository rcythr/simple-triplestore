use crate::{prelude::*, PropertyType};

use super::MemTripleStore;

impl<NodeProperties: PropertyType, EdgeProperties: PropertyType>
    TripleStoreQuery<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    type QueryResultError = ();

    fn run(
        &self,
        query: Query,
    ) -> Result<
        MemTripleStore<NodeProperties, EdgeProperties>,
        QueryError<Self::Error, Self::QueryResultError>,
    > {
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

            Query::SPO(triples) => {
                let mut result = MemTripleStore::new();
                for (sub, pred, obj) in triples.into_iter() {
                    let triple = Triple { sub, pred, obj };
                    if let Some(data_id) = self.spo_data.get(&triple.encode_spo()) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result
                                .insert_edge(triple, data.clone())
                                .map_err(|e| QueryError::Right(e))?;
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
                            result
                                .insert_edge(Triple::decode_spo(&key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
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
                            result
                                .insert_edge(Triple::decode_spo(&key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
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
                            result
                                .insert_edge(Triple::decode_osp(key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
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
                            result
                                .insert_edge(Triple::decode_pos(key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
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
                            result
                                .insert_edge(Triple::decode_pos(key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
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
                            result
                                .insert_edge(Triple::decode_osp(key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
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
    use crate::prelude::*;

    #[test]
    fn test_query_node_props() {
        crate::conformance::query::test_query_node_props(MemTripleStore::new());
    }

    #[test]
    fn test_query_edge_props() {
        crate::conformance::query::test_query_edge_props(MemTripleStore::new());
    }

    #[test]
    fn test_query_s() {
        crate::conformance::query::test_query_s(MemTripleStore::new());
    }

    #[test]
    fn test_query_sp() {
        crate::conformance::query::test_query_sp(MemTripleStore::new());
    }

    #[test]
    fn test_query_p() {
        crate::conformance::query::test_query_p(MemTripleStore::new());
    }

    #[test]
    fn test_query_po() {
        crate::conformance::query::test_query_po(MemTripleStore::new());
    }

    #[test]
    fn test_query_o() {
        crate::conformance::query::test_query_o(MemTripleStore::new());
    }

    #[test]
    fn test_query_os() {
        crate::conformance::query::test_query_os(MemTripleStore::new());
    }
}
