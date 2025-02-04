use crate::{
    traits::{ConcreteIdType, Mergeable, Property},
    MergeError, Triple,
};

use super::{MemTripleStore, TripleStore, TripleStoreMerge};

impl<Id: ConcreteIdType, NodeProps: Property + Mergeable, EdgeProps: Property + Mergeable>
    MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn merge_edge_create_data(
        &mut self,
        old_edge_data_id: Option<Id>,
        new_edge_data: EdgeProps,
    ) -> Id {
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

impl<Id: ConcreteIdType, NodeProps: Property + Mergeable, EdgeProps: Property + Mergeable>
    TripleStoreMerge<Id, NodeProps, EdgeProps> for MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn merge<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<Id, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(crate::EdgeOrder::SPO);

        for r in other_nodes {
            let (id, data) = r.map_err(|e| MergeError::Right(e))?;
            self.merge_node(id, data).map_err(|e| MergeError::Left(e))?;
        }

        for r in other_edges {
            let (id, other_edge_props) = r.map_err(|e| MergeError::Right(e))?;
            self.merge_edge(id, other_edge_props)
                .map_err(|e| MergeError::Left(e))?;
        }

        Ok(())
    }

    fn merge_node(&mut self, node: Id, data: NodeProps) -> Result<(), ()> {
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

    fn merge_edge(&mut self, triple: Triple<Id>, data: EdgeProps) -> Result<(), ()> {
        let old_edge_data_id = match self.spo_data.entry(Id::encode_spo_triple(&triple)) {
            std::collections::btree_map::Entry::Vacant(_) => None,
            std::collections::btree_map::Entry::Occupied(o) => Some(o.get().clone()),
        };

        let new_edge_data_id = self.merge_edge_create_data(old_edge_data_id, data);

        self.insert_edge_data_internal(&triple, &new_edge_data_id);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{MemTripleStore, UlidIdGenerator};

    #[test]
    fn test_merge() {
        crate::conformance::merge::test_merge(|| MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_merge_node() {
        crate::conformance::merge::test_merge_node(|| MemTripleStore::new(UlidIdGenerator::new()));
    }

    #[test]
    fn test_merge_edge() {
        crate::conformance::merge::test_merge_edge(|| MemTripleStore::new(UlidIdGenerator::new()));
    }
}
