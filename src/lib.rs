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

use itertools::Itertools;
use ulid::Ulid;

mod mem;
mod mergeable;
pub mod prelude;
mod query_impl;
mod triple;

#[cfg(test)]
mod conformance;

#[cfg(feature = "sled")]
pub mod sled;

pub use crate::mem::MemTripleStore;
pub use crate::mergeable::Mergeable;
pub use crate::query_impl::Query;
#[cfg(feature = "sled")]
pub use crate::sled::SledTripleStore;
pub use crate::triple::{PropsTriple, Triple};

/// A trait representing a graph constructed of vertices and edges, collectively referred to as nodes.
///
/// Nodes and Edges may be annotated with any type which supports to [PropertyType].
///
/// By default includes:
///   * [Insert][TripleStoreInsert]
///   * [Remove][TripleStoreRemove]
///   * [Iter][TripleStoreIter]
///   * [IntoIter][TripleStoreIntoIter]
///   * [Query][TripleStoreQuery]
///   * [Extend][TripleStoreExtend]
///
/// Some implementations may also support:
///   * [Merge][TripleStoreMerge]
///   * [Set Operations][TripleStoreSetOps]
///
/// # Example
///
/// See [MemTripleStore] or [SledTripleStore] for usage.
pub trait TripleStore<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
    TripleStoreInsert<NodeProperties, EdgeProperties>
    + TripleStoreRemove<NodeProperties, EdgeProperties>
    + TripleStoreIter<NodeProperties, EdgeProperties>
    + TripleStoreIntoIter<NodeProperties, EdgeProperties>
    + TripleStoreQuery<NodeProperties, EdgeProperties>
    + TripleStoreExtend<NodeProperties, EdgeProperties>
{
    fn try_eq<OError: std::fmt::Debug>(
        &self,
        other: &impl TripleStore<NodeProperties, EdgeProperties, Error = OError>,
    ) -> Result<bool, crate::TryEqError<Self::Error, OError>> {
        let (self_nodes, self_edges) = self.iter_nodes(EdgeOrder::SPO);
        let self_nodes = self_nodes.map(|r| r.map_err(|e| TryEqError::Left(e)));
        let self_edges = self_edges.map(|r| r.map_err(|e| TryEqError::Left(e)));

        let (other_nodes, other_edges) = other.iter_nodes(EdgeOrder::SPO);
        let other_nodes = other_nodes.map(|r| r.map_err(|e| TryEqError::Right(e)));
        let other_edges = other_edges.map(|r| r.map_err(|e| TryEqError::Right(e)));

        for zip in self_nodes.zip_longest(other_nodes) {
            match zip {
                itertools::EitherOrBoth::Both(left, right) => {
                    let left = left?;
                    let right = right?;
                    if left != right {
                        return Ok(false);
                    }
                }
                _ => {
                    return Ok(false);
                }
            }
        }

        for zip in self_edges.zip_longest(other_edges) {
            match zip {
                itertools::EitherOrBoth::Both(left, right) => {
                    let left = left?;
                    let right = right?;
                    if left != right {
                        return Ok(false);
                    }
                }
                _ => {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }
}

#[derive(Debug)]
pub enum TryEqError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    Left(LeftError),
    Right(RightError),
}

// Marker trait for all types which are supported as TripleStore properties.
pub trait PropertyType: Clone + std::fmt::Debug + PartialEq {}
impl<T: Clone + std::fmt::Debug + PartialEq> PropertyType for T {}

/// A trait that encapsulates the error type used by other traits in the library.
pub trait TripleStoreError {
    type Error: std::fmt::Debug;
}

/// A trait for insertion operations in [TripleStore]s.
///
/// Allows insertion of vertices (nodes) and edges, both singularly and in batches.
pub trait TripleStoreInsert<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
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
pub trait TripleStoreRemove<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
    TripleStoreError
{
    /// Remove the node with `id`.
    fn remove_node(&mut self, id: impl Borrow<Ulid>) -> Result<(), Self::Error>;

    /// Remove the nodes with the given `ids`.
    fn remove_node_batch<I: IntoIterator<Item = impl Borrow<Ulid>>>(
        &mut self,
        ids: I,
    ) -> Result<(), Self::Error>;

    /// Remove the node with `triple`.
    fn remove_edge(&mut self, triple: Triple) -> Result<(), Self::Error>;

    /// Remove the nodes with the given `triples`.
    fn remove_edge_batch<I: IntoIterator<Item = Triple>>(
        &mut self,
        triples: I,
    ) -> Result<(), Self::Error>;
}

pub enum EdgeOrder {
    SPO,
    POS,
    OSP,
}

impl Default for EdgeOrder {
    fn default() -> Self {
        Self::SPO
    }
}

// Iteration functions which do not consume the TripleStore.
pub trait TripleStoreIter<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
    TripleStoreError
{
    // Return the identifiers for all verticies. The result is lifted out of the iterator for easy usage by the caller.
    fn vertices(&self) -> Result<impl Iterator<Item = Ulid>, Self::Error>;

    // Return two iterators: one for vertices, and one for edges.
    fn iter_nodes(
        &self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>,
        impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>,
    );

    /// Iterate over vertices in the triplestore.
    fn iter_vertices<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>> + 'a;

    /// Iterate over the edges in the triplestore, fetching node properties for each subject and object.
    fn iter_edges_with_props<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>> + 'a;

    /// Iterate over the edges in the triplestore
    fn iter_edges<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>> + 'a;
}

pub trait TripleStoreIntoIter<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
    TripleStoreError
{
    // Return two iterators: one for vertices, and one for edges.
    fn into_iter_nodes(
        self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>,
        impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>,
    );

    /// Iterate over vertices in the triplestore.
    fn into_iter_vertices(
        self,
    ) -> impl Iterator<Item = Result<(Ulid, NodeProperties), Self::Error>>;

    /// Iterate over the edges in the triplestore, fetching node properties for each subject and object.
    fn into_iter_edges_with_props(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<NodeProperties, EdgeProperties>, Self::Error>>;

    /// Iterate over the edges in the triplestore
    fn into_iter_edges(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple, EdgeProperties), Self::Error>>;
}

#[derive(Debug)]
pub enum QueryError<SourceError: std::fmt::Debug, ResultError: std::fmt::Debug> {
    Left(SourceError),
    Right(ResultError),
}

/// A trait for querying operations in a [TripleStore].
///
/// Supports arbitrary source, predicate, and object queries, as well as lookups for properties of nodes and edges.
pub trait TripleStoreQuery<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
    TripleStoreError
{
    /// The result type of a query.
    type QueryResult: TripleStore<NodeProperties, EdgeProperties>;
    type QueryResultError: std::fmt::Debug;

    /// Execute a query and return the result.
    fn run(
        &self,
        query: Query,
    ) -> Result<Self::QueryResult, QueryError<Self::Error, Self::QueryResultError>>;
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

/// A trait for basic set operations in a memory-based [TripleStore].
///
/// Provides functionality for union, intersection, and difference operations on sets of triples.
pub trait TripleStoreSetOps<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
    TripleStoreError
{
    /// The result type for set operations.
    type SetOpsResult: TripleStore<NodeProperties, EdgeProperties>;
    type SetOpsResultError: std::fmt::Debug;

    /// Set union of properties and triples with another [TripleStore].
    fn union<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;

    /// Set intersection of properties and triples with another [TripleStore].
    fn intersection<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;

    /// Set difference of properties triples with another [TripleStore].
    fn difference<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>>;
}

/// Wrapper for errors resulting from [TripleStoreExtend::extend()]
#[derive(Debug)]
pub enum ExtendError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    /// Error from the [TripleStore] being extended.
    Left(LeftError),

    /// Error from the [TripleStore] being consumed.
    Right(RightError),
}

/// A trait for extending a [TripleStore] with elements from another [TripleStore].
///
/// Inserts all nodes and edges from `other` into this [TripleStore], replacing existing property data if present.
pub trait TripleStoreExtend<NodeProperties: PropertyType, EdgeProperties: PropertyType>:
    TripleStoreError
{
    /// Extend this [TripleStore] with nodes and edges from `other`.
    ///
    /// Property data for existing nodes will be replaced with data from `other`.
    fn extend<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), ExtendError<Self::Error, E>>;
}

/// Wrapper for errors resulting from [TripleStoreMerge::merge()]
#[derive(Debug)]
pub enum MergeError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    /// Error from the [TripleStore] being merged _into_.
    Left(LeftError),

    /// Error from the [TripleStore] being merged _from_.
    Right(RightError),
}

/// A trait for supporting merging in [TripleStore]s.
///
/// If `NodeProperties` and `EdgeProperties` support the [Mergeable] trait, this trait provides functionality to
/// merge elements from another [TripleStore], merging properties rather than replacing them.
pub trait TripleStoreMerge<
    NodeProperties: PropertyType + Mergeable,
    EdgeProperties: PropertyType + Mergeable,
>: TripleStoreError
{
    /// Merge all elements from `other` into this [TripleStore].
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
