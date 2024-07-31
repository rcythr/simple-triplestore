use std::borrow::Borrow;

use crate::{prelude::*, IdType, Property};

/// Removal operations for TripleStores.
pub trait TripleStoreRemove<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreError
{
    /// Remove the node with `id`.
    fn remove_node(&mut self, id: impl Borrow<Id>) -> Result<(), Self::Error>;

    /// Remove the node with `triple`.
    fn remove_edge(&mut self, triple: Triple<Id>) -> Result<(), Self::Error>;
}
