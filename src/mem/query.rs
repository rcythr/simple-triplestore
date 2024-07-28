use crate::{PropertiesType, TripleStoreQuery};
use crate::{Query, Triple, TripleStoreInsert};

use super::MemTripleStore;

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>
    TripleStoreQuery<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type QueryResult = MemTripleStore<NodeProperties, EdgeProperties>;
    fn query(
        &mut self,
        query: Query,
    ) -> Result<MemTripleStore<NodeProperties, EdgeProperties>, Self::Error> {
        Ok(match query {
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
    #[test]
    fn test_query_node_props() {
        todo!()
    }

    #[test]
    fn test_query_edge_props() {
        todo!()
    }

    #[test]
    fn test_query_s() {
        todo!()
    }

    #[test]
    fn test_query_sp() {
        todo!()
    }

    #[test]
    fn test_query_p() {
        todo!()
    }

    #[test]
    fn test_query_po() {
        todo!()
    }

    #[test]
    fn test_query_o() {
        todo!()
    }
}
