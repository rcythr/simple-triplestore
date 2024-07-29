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
//! Each node and edge is assigned an [Ulid]. Property data is then associated with this id using key-value storage.
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
//!   * [Memory][MemTripleStore] ( via [std::collections::BTreeMap] )
//!
//!   * [Sled][SledTripleStore]
//!

use std::borrow::Borrow;

use ulid::Ulid;

mod mem;
mod mergeable;
pub mod prelude;
mod query;
mod triple;

#[cfg(feature = "sled")]
mod sled;

pub use crate::mem::MemTripleStore;
pub use crate::mergeable::Mergeable;
pub use crate::query::Query;
#[cfg(feature = "sled")]
pub use crate::sled::SledTripleStore;
pub use crate::triple::{PropsTriple, Triple};

pub trait PropertiesType: Clone + std::fmt::Debug + PartialEq {}
impl<T: Clone + std::fmt::Debug + PartialEq> PropertiesType for T {}

/// A trait that encapsulates the error type used by other traits in the library.
pub trait TripleStoreError {
    type Error: std::fmt::Debug;
}

/// A trait representing a graph constructed of vertices and edges, collectively referred to as nodes.
///
/// Nodes can be annotated with properties of arbitrary types as long as they conform to the `PropertiesType` trait.
/// Edges can also be annotated with properties conforming to the `PropertiesType` trait.
///
/// Triple stores support insertion, removal, iteration, querying, extension, and merging.
pub trait TripleStore<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreInsert<NodeProperties, EdgeProperties>
    + TripleStoreRemove<NodeProperties, EdgeProperties>
    + TripleStoreIter<NodeProperties, EdgeProperties>
    + TripleStoreIntoIter<NodeProperties, EdgeProperties>
    + TripleStoreQuery<NodeProperties, EdgeProperties>
    + TripleStoreExtend<NodeProperties, EdgeProperties>
{
}

/// A trait for insertion operations in triple stores.
///
/// Allows insertion of vertices (nodes) and edges, both singularly and in batches.
pub trait TripleStoreInsert<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreError
{
    /// Insert a node with `id` and `props`.
    fn insert_node(&mut self, id: Ulid, props: NodeProperties) -> Result<(), Self::Error>;

    /// Insert a collection of nodes with (id, props).
    ///
    /// Implementations may either optimize batch insertion or repeatedly call `insert_node`.
    fn insert_node_batch<I>(&mut self, nodes: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = (Ulid, NodeProperties)>;

    /// Insert an edge with `triple` and `props`.
    ///
    /// <div class="warning">Nodes need not be inserted before edges; however, Orphaned edges (edges referring to missing nodes) are ignored
    /// by iteration functions and higher-order operations.</div>
    fn insert_edge(&mut self, triple: Triple, props: EdgeProperties) -> Result<(), Self::Error>;

    /// Insert a collection of edges with (triple, props).
    ///
    /// Implementations may either optimize batch insertion or iteratively call `insert_edge`.
    ///
    /// <div class="warning">Nodes need not be inserted before edges; however, orphaned edges (edges referring to missing nodes) are ignored
    /// by iteration functions and higher-order operations.</div>
    fn insert_edge_batch<I>(&mut self, triples: I) -> Result<(), Self::Error>
    where
        I: IntoIterator<Item = (Triple, EdgeProperties)>;
}

/// Removal operations for TripleStores.
pub trait TripleStoreRemove<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreError
{
    /// Remove the
    fn remove_node(&mut self, node: impl Borrow<Ulid>) -> Result<(), Self::Error>;

    ///
    fn remove_node_batch<I: IntoIterator<Item = Ulid>>(
        &mut self,
        nodes: I,
    ) -> Result<(), Self::Error>;

    ///
    fn remove_edge(&mut self, triple: Triple) -> Result<(), Self::Error>;

    ///
    fn remove_edge_batch<I: IntoIterator<Item = Triple>>(
        &mut self,
        triples: I,
    ) -> Result<(), Self::Error>;
}

// Iteration functions which do not consume the TripleStore.
pub trait TripleStoreIter<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreError
{
    /// Iterate over the edges in the triplestore
    fn iter_spo<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>> + 'a;

    ///
    fn iter_pos<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>> + 'a;

    ///
    fn iter_osp<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>> + 'a;

    ///
    fn iter_node<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>> + 'a;

    ///
    fn iter_edge_spo<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> + 'a;

    ///
    fn iter_edge_pos<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> + 'a;

    ///
    fn iter_edge_osp<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> + 'a;
}

pub trait TripleStoreIntoIter<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreError
{
    //
    fn into_iters(
        self,
    ) -> (
        impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>,
        impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>,
    );

    ///
    fn into_iter_spo(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>;

    ///
    fn into_iter_pos(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>;

    ///
    fn into_iter_osp(
        self,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>;

    ///
    fn into_iter_node(self) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>;

    ///
    fn into_iter_edge_spo(
        self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>;

    ///
    fn into_iter_edge_pos(
        self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>;

    ///
    fn into_iter_edge_osp(
        self,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>;
}

/// A trait for querying operations in a triple store.
///
/// Supports arbitrary source, predicate, and object queries, as well as lookups for properties of nodes and edges.
pub trait TripleStoreQuery<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreError
{
    /// The result type of a query.
    type QueryResult;

    /// Execute a query and return the result.
    fn run(&self, query: Query) -> Result<Self::QueryResult, Self::Error>;
}

#[derive(Debug)]
pub enum SetOpsError<
    LeftError: std::fmt::Debug,
    RightError: std::fmt::Debug,
    ResultError: std::fmt::Debug,
> {
    Left(LeftError),
    Right(RightError),
    Result(ResultError),
}

/// A trait for basic set operations in a memory-based triple store.
///
/// Provides functionality for union, intersection, and difference operations on sets of triples.
pub trait TripleStoreSetOps<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreError
{
    /// The result type for set operations.
    type SetOpsResult: TripleStore<NodeProperties, EdgeProperties>;
    type SetOpsResultError: std::fmt::Debug;

    /// Set union of properties and triples with another triple store.
    fn union<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;

    /// Set intersection of properties and triples with another triple store.
    fn intersection<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;

    /// Set difference of properties triples with another triple store.
    fn difference<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;
}

#[derive(Debug)]
pub enum ExtendError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    Left(LeftError),
    Right(RightError),
}

/// A trait for extending a triple store with elements from another triple store.
///
/// Inserts all nodes and edges from `other` into this triple store, replacing existing property data if present.
pub trait TripleStoreExtend<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>:
    TripleStoreError
{
    /// Extend this triple store with nodes and edges from `other`.
    ///
    /// Property data for existing nodes will be replaced with data from `other`.
    fn extend<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), ExtendError<Self::Error, E>>;
}

#[derive(Debug)]
pub enum MergeError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    Left(LeftError),
    Right(RightError),
}

/// A trait for merging operations in triple stores.
///
/// If `NodeProperties` and `EdgeProperties` support the [Mergeable] trait, this trait provides functionality to
/// merge elements from another triple store, merging properties rather than replacing them.
pub trait TripleStoreMerge<
    NodeProperties: PropertiesType + Mergeable,
    EdgeProperties: PropertiesType + Mergeable,
>: TripleStoreError
{
    /// Merge all elements from `other` into this triple store.
    ///
    /// Duplicate elements will be merged using the `Mergeable` trait's merge operation.
    fn merge<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), MergeError<Self::Error, E>>;

    /// Merge a single node with `id` and `props`.
    fn merge_node(&mut self, node: Ulid, props: NodeProperties) -> Result<(), Self::Error>;

    //// Merge a collection of nodes with `(id, props)`.
    ///
    /// Implementations may optimize batch merging, or may simply invoke `merge_node` repeatedly.
    fn merge_node_batch<I: IntoIterator<Item = (Ulid, NodeProperties)>>(
        &mut self,
        nodes: I,
    ) -> Result<(), Self::Error>;

    //// Merge a collection of edges with `(id, props)`.
    fn merge_edge(&mut self, triple: Triple, props: EdgeProperties) -> Result<(), Self::Error>;

    /// Merge a collection of edges with `(triple, props)`.
    ///
    /// Implementations may optimize batch merging, or may simply invoke `merge_node` repeatedly.
    fn merge_edge_batch<I: IntoIterator<Item = (Triple, EdgeProperties)>>(
        &mut self,
        triples: I,
    ) -> Result<(), Self::Error>;
}
