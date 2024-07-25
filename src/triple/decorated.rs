use super::Triple;

#[cfg(feature = "rdf")]
use super::RdfTriple;

// A triple along with the associated Node and Edge properties.
pub struct DecoratedTriple<NodeProperties, EdgeProperties> {
    pub triple: Triple,

    pub sub_data: NodeProperties,
    pub pred_data: EdgeProperties,
    pub obj_data: NodeProperties,
}

// An rdf triple along with the associated Node and Edge properties.
#[cfg(feature = "rdf")]
pub struct DecoratedRdfTriple<NodeProperties, EdgeProperties> {
    pub triple: RdfTriple,

    pub sub_data: NodeProperties,
    pub pred_data: EdgeProperties,
    pub obj_data: NodeProperties,
}
