use ulid::Ulid;

use crate::{DecoratedTriple, Triple, TripleStoreIntoIter};

use super::MemTripleStore;

impl<'a, NodeProperties: Clone + 'a, EdgeProperties: Clone + 'a>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    fn iter_spo(
        &'a self,
    ) -> impl Iterator<Item = Result<DecoratedTriple<&NodeProperties, &EdgeProperties>, ()>> {
        self.spo_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_spo(&k);

            let sub_data = self.node_props.get(&triple.sub);
            let pred_data = self.edge_props.get(v);
            let obj_data = self.node_props.get(&triple.obj);

            match (sub_data, pred_data, obj_data) {
                (Some(sub_data), Some(pred_data), Some(obj_data)) => Some(Ok(DecoratedTriple {
                    triple,
                    sub_data,
                    obj_data,
                    pred_data,
                })),
                _ => None,
            }
        })
    }

    fn iter_pos(
        &'a self,
    ) -> impl Iterator<Item = Result<DecoratedTriple<&NodeProperties, &EdgeProperties>, ()>> {
        self.pos_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_pos(&k);

            let sub_data = self.node_props.get(&triple.sub);
            let pred_data = self.edge_props.get(v);
            let obj_data = self.node_props.get(&triple.obj);

            match (sub_data, pred_data, obj_data) {
                (Some(sub_data), Some(pred_data), Some(obj_data)) => Some(Ok(DecoratedTriple {
                    triple,
                    sub_data,
                    obj_data,
                    pred_data,
                })),
                _ => None,
            }
        })
    }

    fn iter_osp(
        &self,
    ) -> impl Iterator<Item = Result<DecoratedTriple<&NodeProperties, &EdgeProperties>, ()>> {
        self.osp_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_osp(&k);

            let sub_data = self.node_props.get(&triple.sub);
            let pred_data = self.edge_props.get(v);
            let obj_data = self.node_props.get(&triple.obj);

            match (sub_data, pred_data, obj_data) {
                (Some(sub_data), Some(pred_data), Some(obj_data)) => Some(Ok(DecoratedTriple {
                    triple,
                    sub_data,
                    obj_data,
                    pred_data,
                })),
                _ => None,
            }
        })
    }

    fn iter_edge_spo(&'a self) -> impl Iterator<Item = Result<(Triple, &EdgeProperties), ()>> {
        self.spo_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_spo(&k), v))),
                None => None,
            })
    }

    fn _iter_edge_pos(&'a self) -> impl Iterator<Item = Result<(Triple, &EdgeProperties), ()>> {
        self.pos_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_pos(&k), v))),
                None => None,
            })
    }

    fn iter_edge_osp(&'a self) -> impl Iterator<Item = Result<(Triple, &EdgeProperties), ()>> {
        self.osp_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some(Ok((Triple::decode_osp(&k), v))),
                None => None,
            })
    }

    fn iter_node(&'a self) -> impl Iterator<Item = Result<(&Ulid, &NodeProperties), ()>> {
        self.node_props.iter().map(|o| Ok(o))
    }
}

impl<NodeProperties: Clone, EdgeProperties: Clone>
    TripleStoreIntoIter<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    fn into_iter_spo(
        self,
    ) -> impl Iterator<Item = Result<DecoratedTriple<NodeProperties, EdgeProperties>, ()>> {
        self.spo_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_spo(&k);

            let sub_data = self.node_props.get(&triple.sub).map(|o| o.clone());
            let obj_data = self.node_props.get(&triple.obj).map(|o| o.clone());
            let pred_data = self.edge_props.get(&v).map(|o| o.clone());

            match (sub_data, obj_data, pred_data) {
                (Some(sub_data), Some(obj_data), Some(pred_data)) => Some(Ok(DecoratedTriple {
                    sub_data,
                    obj_data,
                    pred_data,
                    triple,
                })),
                _ => None,
            }
        })
    }

    fn into_iter_pos(
        self,
    ) -> impl Iterator<Item = Result<DecoratedTriple<NodeProperties, EdgeProperties>, ()>> {
        self.pos_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_pos(&k);

            let sub_data = self.node_props.get(&triple.sub).map(|o| o.clone());
            let obj_data = self.node_props.get(&triple.obj).map(|o| o.clone());
            let pred_data = self.edge_props.get(&v).map(|o| o.clone());

            match (sub_data, obj_data, pred_data) {
                (Some(sub_data), Some(obj_data), Some(pred_data)) => Some(Ok(DecoratedTriple {
                    sub_data,
                    obj_data,
                    pred_data,
                    triple,
                })),
                _ => None,
            }
        })
    }

    fn into_iter_osp(
        self,
    ) -> impl Iterator<Item = Result<DecoratedTriple<NodeProperties, EdgeProperties>, ()>> {
        self.osp_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_osp(&k);

            let sub_data = self.node_props.get(&triple.sub).map(|o| o.clone());
            let obj_data = self.node_props.get(&triple.obj).map(|o| o.clone());
            let pred_data = self.edge_props.get(&v).map(|o| o.clone());

            match (sub_data, obj_data, pred_data) {
                (Some(sub_data), Some(obj_data), Some(pred_data)) => Some(Ok(DecoratedTriple {
                    sub_data,
                    obj_data,
                    pred_data,
                    triple,
                })),
                _ => None,
            }
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
