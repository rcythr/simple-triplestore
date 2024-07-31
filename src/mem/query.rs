use crate::{
    prelude::*,
    traits::{IdType, Property},
    Query, QueryError, Triple,
};

use super::MemTripleStore;

impl<Id: IdType, NodeProps: Property, EdgeProps: Property>
    TripleStoreQuery<Id, NodeProps, EdgeProps> for MemTripleStore<Id, NodeProps, EdgeProps>
{
    type QueryResult = MemTripleStore<Id, NodeProps, EdgeProps>;
    type QueryResultError = ();

    fn run(
        &self,
        query: Query<Id>,
    ) -> Result<
        MemTripleStore<Id, NodeProps, EdgeProps>,
        QueryError<Self::Error, Self::QueryResultError>,
    > {
        Ok(match query {
            Query::NodeProps(nodes) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for node in nodes {
                    if let Some(data) = self.node_props.get(&node) {
                        result.node_props.insert(node, data.clone());
                    }
                }
                result
            }

            Query::SPO(triples) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (sub, pred, obj) in triples.into_iter() {
                    let triple = Triple { sub, pred, obj };
                    if let Some(data_id) = self.spo_data.get(&Id::encode_spo_triple(&triple)) {
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
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for sub in items {
                    for (key, data_id) in self.spo_data.range(Id::key_bounds_1(sub)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result
                                .insert_edge(Id::decode_spo_triple(&key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::SP(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (sub, pred) in items {
                    for (key, data_id) in self.spo_data.range(Id::key_bounds_2(sub, pred)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result
                                .insert_edge(Id::decode_spo_triple(&key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::SO(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (sub, obj) in items {
                    for (key, data_id) in self.osp_data.range(Id::key_bounds_2(obj, sub)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result
                                .insert_edge(Id::decode_osp_triple(key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::P(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for pred in items {
                    for (key, data_id) in self.pos_data.range(Id::key_bounds_1(pred)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result
                                .insert_edge(Id::decode_pos_triple(key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::PO(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for (pred, obj) in items {
                    for (key, data_id) in self.pos_data.range(Id::key_bounds_2(pred, obj)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result
                                .insert_edge(Id::decode_pos_triple(key), data.clone())
                                .map_err(|e| QueryError::Right(e))?;
                        }
                    }
                }
                result
            }

            Query::O(items) => {
                let mut result =
                    MemTripleStore::new_from_boxed_id_generator(self.id_generator.clone());
                for obj in items {
                    for (key, data_id) in self.osp_data.range(Id::key_bounds_1(obj)) {
                        if let Some(data) = self.edge_props.get(&data_id) {
                            result
                                .insert_edge(Id::decode_osp_triple(key), data.clone())
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
    use crate::{MemTripleStore, UlidIdGenerator};

    #[test]
    fn test_query_node_props() {
        crate::conformance::query::test_query_node_props(MemTripleStore::new(
            UlidIdGenerator::new(),
        ));
    }

    #[test]
    fn test_query_edge_props() {
        crate::conformance::query::test_query_edge_props(MemTripleStore::new(
            UlidIdGenerator::new(),
        ));
    }

    #[test]
    fn test_query_s() {
        crate::conformance::query::test_query_s(MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_query_sp() {
        crate::conformance::query::test_query_sp(MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_query_p() {
        crate::conformance::query::test_query_p(MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_query_po() {
        crate::conformance::query::test_query_po(MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_query_o() {
        crate::conformance::query::test_query_o(MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_query_os() {
        crate::conformance::query::test_query_os(MemTripleStore::new(UlidIdGenerator::new()));
    }
}
