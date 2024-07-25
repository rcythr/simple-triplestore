use ulid::Ulid;

use crate::{DecoratedTriple, Triple};

use super::MemTripleStore;

impl<'a, NodeProperties: Clone + 'a, EdgeProperties: Clone + 'a>
    MemTripleStore<NodeProperties, EdgeProperties>
{
    pub(super) fn handle_iter_spo(
        &'a self,
    ) -> impl Iterator<Item = DecoratedTriple<&NodeProperties, &EdgeProperties>> {
        self.spo_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_spo(&k);

            let sub_data = self.node_props.get(&triple.sub);
            let pred_data = self.edge_props.get(v);
            let obj_data = self.node_props.get(&triple.obj);

            match (sub_data, pred_data, obj_data) {
                (Some(sub_data), Some(pred_data), Some(obj_data)) => Some(DecoratedTriple {
                    triple,
                    sub_data,
                    obj_data,
                    pred_data,
                }),
                _ => None,
            }
        })
    }

    pub(super) fn handle_iter_pos(
        &'a self,
    ) -> impl Iterator<Item = DecoratedTriple<&NodeProperties, &EdgeProperties>> {
        self.pos_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_pos(&k);

            let sub_data = self.node_props.get(&triple.sub);
            let pred_data = self.edge_props.get(v);
            let obj_data = self.node_props.get(&triple.obj);

            match (sub_data, pred_data, obj_data) {
                (Some(sub_data), Some(pred_data), Some(obj_data)) => Some(DecoratedTriple {
                    triple,
                    sub_data,
                    obj_data,
                    pred_data,
                }),
                _ => None,
            }
        })
    }

    pub(super) fn handle_iter_osp(
        &self,
    ) -> impl Iterator<Item = DecoratedTriple<&NodeProperties, &EdgeProperties>> {
        self.osp_data.iter().filter_map(|(k, v)| {
            let triple = Triple::decode_osp(&k);

            let sub_data = self.node_props.get(&triple.sub);
            let pred_data = self.edge_props.get(v);
            let obj_data = self.node_props.get(&triple.obj);

            match (sub_data, pred_data, obj_data) {
                (Some(sub_data), Some(pred_data), Some(obj_data)) => Some(DecoratedTriple {
                    triple,
                    sub_data,
                    obj_data,
                    pred_data,
                }),
                _ => None,
            }
        })
    }

    pub(super) fn handle_iter_edge_spo(
        &'a self,
    ) -> impl Iterator<Item = (Triple, &EdgeProperties)> {
        self.spo_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some((Triple::decode_spo(&k), v)),
                None => None,
            })
    }

    pub(super) fn handle_iter_edge_pos(
        &'a self,
    ) -> impl Iterator<Item = (Triple, &EdgeProperties)> {
        self.pos_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some((Triple::decode_pos(&k), v)),
                None => None,
            })
    }

    pub(super) fn handle_iter_edge_osp(
        &'a self,
    ) -> impl Iterator<Item = (Triple, &EdgeProperties)> {
        self.osp_data
            .iter()
            .filter_map(|(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some((Triple::decode_osp(&k), v)),
                None => None,
            })
    }

    pub(super) fn handle_iter_node(&'a self) -> impl Iterator<Item = (&Ulid, &NodeProperties)> {
        self.node_props.iter()
    }
}

impl<NodeProperties: Clone, EdgeProperties: Clone> MemTripleStore<NodeProperties, EdgeProperties> {
    pub(super) fn handle_into_iter_node(self) -> impl Iterator<Item = (Ulid, NodeProperties)> {
        self.node_props.into_iter()
    }

    pub(super) fn handle_into_iter_edge_spo(
        self,
    ) -> impl Iterator<Item = (Triple, EdgeProperties)> {
        self.spo_data
            .into_iter()
            .filter_map(move |(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some((Triple::decode_spo(&k), v.clone())),
                None => None,
            })
    }

    pub(super) fn handle_into_iter_edge_pos(
        self,
    ) -> impl Iterator<Item = (Triple, EdgeProperties)> {
        self.pos_data
            .into_iter()
            .filter_map(move |(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some((Triple::decode_pos(&k), v.clone())),
                None => None,
            })
    }

    pub(super) fn handle_into_iter_edge_osp(
        self,
    ) -> impl Iterator<Item = (Triple, EdgeProperties)> {
        self.osp_data
            .into_iter()
            .filter_map(move |(k, v)| match self.edge_props.get(&v) {
                Some(v) => Some((Triple::decode_osp(&k), v.clone())),
                None => None,
            })
    }

    pub(super) fn handle_into_iter_spo(
        self,
    ) -> impl Iterator<Item = DecoratedTriple<NodeProperties, EdgeProperties>> {
        self.spo_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_spo(&k);

            let sub_data = self.node_props.get(&triple.sub).map(|o| o.clone());
            let obj_data = self.node_props.get(&triple.obj).map(|o| o.clone());
            let pred_data = self.edge_props.get(&v).map(|o| o.clone());

            match (sub_data, obj_data, pred_data) {
                (Some(sub_data), Some(obj_data), Some(pred_data)) => Some(DecoratedTriple {
                    sub_data,
                    obj_data,
                    pred_data,
                    triple,
                }),
                _ => None,
            }
        })
    }

    pub(super) fn handle_into_iter_pos(
        self,
    ) -> impl Iterator<Item = DecoratedTriple<NodeProperties, EdgeProperties>> {
        self.pos_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_pos(&k);

            let sub_data = self.node_props.get(&triple.sub).map(|o| o.clone());
            let obj_data = self.node_props.get(&triple.obj).map(|o| o.clone());
            let pred_data = self.edge_props.get(&v).map(|o| o.clone());

            match (sub_data, obj_data, pred_data) {
                (Some(sub_data), Some(obj_data), Some(pred_data)) => Some(DecoratedTriple {
                    sub_data,
                    obj_data,
                    pred_data,
                    triple,
                }),
                _ => None,
            }
        })
    }

    pub(super) fn handle_into_iter_osp(
        self,
    ) -> impl Iterator<Item = DecoratedTriple<NodeProperties, EdgeProperties>> {
        self.osp_data.into_iter().filter_map(move |(k, v)| {
            let triple = Triple::decode_osp(&k);

            let sub_data = self.node_props.get(&triple.sub).map(|o| o.clone());
            let obj_data = self.node_props.get(&triple.obj).map(|o| o.clone());
            let pred_data = self.edge_props.get(&v).map(|o| o.clone());

            match (sub_data, obj_data, pred_data) {
                (Some(sub_data), Some(obj_data), Some(pred_data)) => Some(DecoratedTriple {
                    sub_data,
                    obj_data,
                    pred_data,
                    triple,
                }),
                _ => None,
            }
        })
    }
}
