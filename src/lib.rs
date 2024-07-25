//!
//! ```text
//!            ██████╗  ██████╗██╗   ██╗████████╗██╗  ██╗██████╗  █╗ ███████╗
//!            ██╔══██╗██╔════╝╚██╗ ██╔╝╚══██╔══╝██║  ██║██╔══██╗ ╚╝ ██╔════╝
//!            ██████╔╝██║      ╚████╔╝    ██║   ███████║██████╔╝    ███████╗
//!            ██╔══██╗██║       ╚██╔╝     ██║   ██╔══██║██╔══██╗    ╚════██║
//!            ██║  ██║╚██████╗   ██║      ██║   ██║  ██║██║  ██║    ███████║
//!            ╚═╝  ╚═╝ ╚═════╝   ╚═╝      ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝    ╚══════╝
//!
//!                          ____ _ _  _ ___  _    ____
//!                          [__  | |\/| |__] |    |___
//!                          ___] | |  | |    |___ |___
//!
//!                                                 
//!  ████████╗██████╗ ██╗██████╗ ██╗     ███████╗███████╗████████╗ ██████╗ ██████╗ ███████╗
//!  ╚══██╔══╝██╔══██╗██║██╔══██╗██║     ██╔════╝██╔════╝╚══██╔══╝██╔═══██╗██╔══██╗██╔════╝
//!     ██║   ██████╔╝██║██████╔╝██║     █████╗  ███████╗   ██║   ██║   ██║██████╔╝█████╗  
//!     ██║   ██╔══██╗██║██╔═══╝ ██║     ██╔══╝  ╚════██║   ██║   ██║   ██║██╔══██╗██╔══╝  
//!     ██║   ██║  ██║██║██║     ███████╗███████╗███████║   ██║   ╚██████╔╝██║  ██║███████╗
//!     ╚═╝   ╚═╝  ╚═╝╚═╝╚═╝     ╚══════╝╚══════╝╚══════╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚══════╝
//!```
//!
//!
//! A [triplestore](https://en.wikipedia.org/wiki/Triplestore) which can be used as a flexible graph database with support for custom node and edge properties.
//!
//! ## Data Model
//! Each node and edge is assigned an [Ulid][ulid::Ulid]. Property data is then associated with this id using key-value storage.
//!
//! Graph relationships are stored three times as `(Ulid, Ulid, Ulid) -> Ulid` with the following key orders:
//!   * Subject, Predicate, Object
//!   * Predicate, Object, Subject
//!   * Object, Subject, Predicate
//!
//! This allows for any graph query to be decomposed into a range query on the lookup with the ideal ordering.
//!
//! ## Query
//!
//! ## Supported Key-Value Backends
//!
//!   * [Memory][MemTripleStore] ( via std::collections::HashMap and std::collections::BTreeMap )
//!
//!   * [Sled][SledTripleStore]
//!

use ulid::Ulid;

pub mod mem;
pub mod mergeable;
pub mod query;
pub mod triple;

#[cfg(feature = "sled")]
pub mod sled;

pub use crate::mem::MemTripleStore;
pub use crate::mergeable::Mergeable;
pub use crate::query::Query;
#[cfg(feature = "sled")]
pub use crate::sled::SledTripleStore;
pub use crate::triple::{DecoratedTriple, Triple};

pub trait TripleStore<NodeProperties: Clone, EdgeProperties: Clone> {
    type Error;

    type QueryResult;

    /// Consume `other` and add its nodes and edges to this Triplestore.
    ///
    /// Existing property data will be replaced with property data found in `other`.
    fn extend(&mut self, other: Self);

    // ///
    // fn into_iter_spo(self) -> impl Iterator<Item = DecoratedTriple<NodeProperties, EdgeProperties>>;

    // ///
    // fn into_iter_pos(self) -> impl Iterator<Item = DecoratedTriple<NodeProperties, EdgeProperties>>;

    // ///
    // fn into_iter_osp(self) -> impl Iterator<Item = DecoratedTriple<NodeProperties, EdgeProperties>>;

    // ///
    // fn into_iter_node(self) -> impl Iterator<Item = (Ulid, NodeProperties)>;

    // ///
    // fn into_iter_edge_spo(self) -> impl Iterator<Item = (Triple, EdgeProperties)>;

    // ///
    // fn into_iter_edge_pos(self) -> impl Iterator<Item = (Triple, EdgeProperties)>;

    // ///
    // fn into_iter_edge_osp(self) -> impl Iterator<Item = (Triple, EdgeProperties)>;

    ///
    fn insert_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), Self::Error>;

    ///
    fn insert_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (Ulid, NodeProperties)>,
    ) -> Result<(), Self::Error>;

    ///
    fn insert_edge(&mut self, triple: Triple, data: EdgeProperties) -> Result<(), Self::Error>;

    ///
    fn insert_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (Triple, EdgeProperties)>,
    ) -> Result<(), Self::Error>;

    ///
    fn remove_node(&mut self, node: &Ulid) -> Result<(), Self::Error>;

    ///
    fn remove_node_batch(&mut self, nodes: impl Iterator<Item = Ulid>) -> Result<(), Self::Error>;

    ///
    fn remove_edge(&mut self, triple: Triple) -> Result<(), Self::Error>;

    ///
    fn remove_edge_batch(
        &mut self,
        triples: impl Iterator<Item = Triple>,
    ) -> Result<(), Self::Error>;

    fn query(&mut self, query: Query) -> Result<Self::QueryResult, Self::Error>;
}

pub trait MergableTripleStore<NodeProperties: Clone + Mergeable, EdgeProperties: Clone + Mergeable>:
    TripleStore<NodeProperties, EdgeProperties>
{
    ///
    fn merge(&mut self, other: Self);

    ///
    fn merge_node(&mut self, node: Ulid, data: NodeProperties) -> Result<(), Self::Error>;

    ///
    fn merge_node_batch(
        &mut self,
        nodes: impl Iterator<Item = (Ulid, NodeProperties)>,
    ) -> Result<(), Self::Error>;

    ///
    fn merge_edge(&mut self, triple: Triple, data: EdgeProperties) -> Result<(), Self::Error>;

    ///
    fn merge_edge_batch(
        &mut self,
        triples: impl Iterator<Item = (Triple, EdgeProperties)>,
    ) -> Result<(), Self::Error>;
}
