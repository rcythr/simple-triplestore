use std::collections::BTreeMap;

use crate::{
    prelude::*,
    traits::{ConcreteIdType, Property},
    EdgeOrder, PropsTriple, Triple,
};

use super::MemTripleStore;

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property>
    MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn iter_impl(
        node_props: &BTreeMap<Id, NodeProps>,
        edge_props: &BTreeMap<Id, EdgeProps>,
        triple: Triple<Id>,
        v: &Id,
    ) -> Option<Result<PropsTriple<Id, NodeProps, EdgeProps>, ()>> {
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

impl<Id: ConcreteIdType, NodeProps: Property, EdgeProps: Property> TripleStoreIter<Id, NodeProps, EdgeProps>
    for MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn vertices(&self) -> Result<impl Iterator<Item = Id>, Self::Error> {
        Ok(self.node_props.iter().map(|e| e.0.clone()))
    }

    fn iter_nodes(
        &self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Id, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>>,
    ) {
        (self.iter_vertices(), self.iter_edges(order))
    }

    fn iter_vertices<'a>(&'a self) -> impl Iterator<Item = Result<(Id, NodeProps), ()>> + 'a {
        self.node_props
            .iter()
            .map(|(id, props)| Ok((id.clone(), props.clone())))
    }

    fn iter_edges_with_props<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Id, NodeProps, EdgeProps>, ()>> + 'a {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(
                self.spo_data
                    .iter()
                    .map(|(k, v)| (Id::decode_spo_triple(k), v)),
            ),
            EdgeOrder::POS => Box::new(
                self.pos_data
                    .iter()
                    .map(|(k, v)| (Id::decode_pos_triple(k), v)),
            ),
            EdgeOrder::OSP => Box::new(
                self.osp_data
                    .iter()
                    .map(|(k, v)| (Id::decode_osp_triple(k), v)),
            ),
        };

        edges.filter_map(|(k, v)| {
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, k, &v)
        })
    }

    fn iter_edges<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Id>, EdgeProps), ()>> + 'a {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(
                self.spo_data
                    .iter()
                    .map(|(k, v)| (Id::decode_spo_triple(k), v)),
            ),
            EdgeOrder::POS => Box::new(
                self.pos_data
                    .iter()
                    .map(|(k, v)| (Id::decode_pos_triple(k), v)),
            ),
            EdgeOrder::OSP => Box::new(
                self.osp_data
                    .iter()
                    .map(|(k, v)| (Id::decode_osp_triple(k), v)),
            ),
        };

        edges.filter_map(|(k, v)| match self.edge_props.get(&v) {
            Some(v) => Some(Ok((k, v.clone()))),
            None => None,
        })
    }
}

impl<Id: ConcreteIdType, NodeProps: Property + PartialEq, EdgeProps: Property + PartialEq>
    TripleStoreIntoIter<Id, NodeProps, EdgeProps> for MemTripleStore<Id, NodeProps, EdgeProps>
{
    fn into_iter_nodes(
        self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Id, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Id>, EdgeProps), Self::Error>>,
    ) {
        let node_iter = self.node_props.into_iter().map(|o| Ok(o));
        let edge_iter = {
            let edges: Box<dyn Iterator<Item = _>> = match order {
                EdgeOrder::SPO => Box::new(
                    self.spo_data
                        .into_iter()
                        .map(|(k, v)| (Id::decode_spo_triple(&k), v)),
                ),
                EdgeOrder::POS => Box::new(
                    self.pos_data
                        .into_iter()
                        .map(|(k, v)| (Id::decode_pos_triple(&k), v)),
                ),
                EdgeOrder::OSP => Box::new(
                    self.osp_data
                        .into_iter()
                        .map(|(k, v)| (Id::decode_osp_triple(&k), v)),
                ),
            };

            edges.filter_map(
                move |(k, v): (Triple<Id>, Id)| match self.edge_props.get(&v) {
                    Some(v) => Some(Ok((k, v.clone()))),
                    None => None,
                },
            )
        };
        (node_iter, edge_iter)
    }

    fn into_iter_vertices(self) -> impl Iterator<Item = Result<(Id, NodeProps), ()>> {
        self.node_props.into_iter().map(|o| Ok(o))
    }

    fn into_iter_edges_with_props(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Id, NodeProps, EdgeProps>, ()>> {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(
                self.spo_data
                    .into_iter()
                    .map(|(k, v)| (Id::decode_spo_triple(&k), v)),
            ),
            EdgeOrder::POS => Box::new(
                self.pos_data
                    .into_iter()
                    .map(|(k, v)| (Id::decode_pos_triple(&k), v)),
            ),
            EdgeOrder::OSP => Box::new(
                self.osp_data
                    .into_iter()
                    .map(|(k, v)| (Id::decode_osp_triple(&k), v)),
            ),
        };

        edges.filter_map(move |(k, v): (Triple<Id>, Id)| {
            MemTripleStore::iter_impl(&self.node_props, &self.edge_props, k, &v)
        })
    }

    fn into_iter_edges(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Id>, EdgeProps), ()>> {
        let edges: Box<dyn Iterator<Item = _>> = match order {
            EdgeOrder::SPO => Box::new(
                self.spo_data
                    .into_iter()
                    .map(|(k, v)| (Id::decode_spo_triple(&k), v)),
            ),
            EdgeOrder::POS => Box::new(
                self.pos_data
                    .into_iter()
                    .map(|(k, v)| (Id::decode_pos_triple(&k), v)),
            ),
            EdgeOrder::OSP => Box::new(
                self.osp_data
                    .into_iter()
                    .map(|(k, v)| (Id::decode_osp_triple(&k), v)),
            ),
        };

        edges.filter_map(move |(k, v)| match self.edge_props.get(&v) {
            Some(v) => Some(Ok((k, v.clone()))),
            None => None,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{MemTripleStore, UlidIdGenerator};

    #[test]
    fn test_iter_spo() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_iter_spo(db);
    }

    #[test]
    fn test_iter_pos() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_iter_pos(db);
    }

    #[test]
    fn test_iter_osp() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_iter_osp(db);
    }

    #[test]
    fn test_iter_edge_spo() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_iter_edge_spo(db);
    }

    #[test]
    fn test_iter_edge_pos() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_iter_edge_pos(db);
    }

    #[test]
    fn test_iter_edge_osp() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_iter_edge_osp(db);
    }

    #[test]
    fn test_iter_node() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_iter_node(db);
    }

    #[test]
    fn test_into_iter_spo() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_into_iter_spo(db);
    }

    #[test]
    fn test_into_iter_pos() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_into_iter_pos(db);
    }

    #[test]
    fn test_into_iter_osp() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_into_iter_osp(db);
    }

    #[test]
    fn test_into_iter_edge_spo() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_into_iter_edge_spo(db);
    }

    #[test]
    fn test_into_iter_edge_pos() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_into_iter_edge_pos(db);
    }

    #[test]
    fn test_into_iter_edge_osp() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_into_iter_edge_osp(db);
    }

    #[test]
    fn test_into_iter_node() {
        let db = MemTripleStore::new(UlidIdGenerator::new());
        crate::conformance::iter::test_into_iter_node(db);
    }
}
