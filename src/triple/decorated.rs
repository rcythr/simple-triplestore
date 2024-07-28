use super::Triple;

// A triple along with the associated Node and Edge properties.
#[derive(Debug, Clone, PartialEq)]
pub struct DecoratedTriple<NodeProperties: Clone + PartialEq, EdgeProperties: Clone + PartialEq> {
    pub triple: Triple,
    pub sub_data: NodeProperties,
    pub pred_data: EdgeProperties,
    pub obj_data: NodeProperties,
}
