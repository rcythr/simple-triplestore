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

pub mod prelude;

use std::collections::HashSet;

use itertools::Itertools;

mod id;
mod mergeable;

mod triple;
pub use crate::triple::{PropsTriple, Triple};

mod traits;
use traits::{
    IdType, Mergeable, TripleStoreExtend, TripleStoreInsert, TripleStoreIntoIter, TripleStoreIter,
    TripleStoreQuery, TripleStoreRemove,
};

#[cfg(test)]
mod conformance;

mod mem;
pub use crate::mem::MemTripleStore;

#[cfg(feature = "sled")]
pub mod sled;
#[cfg(feature = "sled")]
pub use crate::sled::SledTripleStore;

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
pub trait TripleStore<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreInsert<Id, NodeProps, EdgeProps>
    + TripleStoreRemove<Id, NodeProps, EdgeProps>
    + TripleStoreIter<Id, NodeProps, EdgeProps>
    + TripleStoreIntoIter<Id, NodeProps, EdgeProps>
    + TripleStoreQuery<Id, NodeProps, EdgeProps>
    + TripleStoreExtend<Id, NodeProps, EdgeProps>
{
    fn try_eq<OError: std::fmt::Debug>(
        &self,
        other: &impl TripleStore<Id, NodeProps, EdgeProps, Error = OError>,
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

// Marker trait for all types which are supported as TripleStore properties.
pub trait Property: Clone + std::fmt::Debug + PartialEq {}
impl<T: Clone + std::fmt::Debug + PartialEq> Property for T {}

/// A trait that encapsulates the error type used by other traits in the library.
pub trait TripleStoreError {
    type Error: std::fmt::Debug;
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug)]
pub enum TryEqError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    Left(LeftError),
    Right(RightError),
}

/// Represents a query which can be executed on a [TripleStore][crate::TripleStore].
///
/// These are most easily created using teh [query][crate::query] macro.
#[derive(Debug, PartialEq, Eq)]
pub enum Query<Id: IdType> {
    /// Fetch the NodeProps for the given set of ids.
    NodeProps(HashSet<Id>),

    /// Fetch the edges for the given set of triples.
    SPO(HashSet<(Id, Id, Id)>),

    /// Fetch all edges which point to one of the given set of ids.
    O(HashSet<Id>),

    /// Fetch all edges which start at one of the given set of ids.
    S(HashSet<Id>),

    /// Fetch all edges which have one of the given set of edge ids.
    P(HashSet<Id>),

    /// Fetch all edges which have one of the given tuples as predicate and object.
    PO(HashSet<(Id, Id)>),

    /// Fetch all edges which have one of the given tuples as subject and object.
    SO(HashSet<(Id, Id)>),

    /// Fetch all edges which have one of the given tuples as subject and predicate.
    SP(HashSet<(Id, Id)>),
}

/// Syntactic sugar macro for constructing [Query] objects which can be used in [crate::TripleStoreQuery::run()].
///
/// # Examples
///
/// The following is used throught all of the examples below:
/// ```
/// use ulid::Ulid;
/// use simple_triplestore::prelude::*;
/// let a = Ulid(1);
/// let b = Ulid(2);
/// let c = Ulid(3);
/// let d = Ulid(4);
/// let e = Ulid(5);
/// let f = Ulid(6);
/// ```
///
/// ### Node Properties
/// To fetch node properties for a collection of vertices:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let a = Ulid(1);
/// # let b = Ulid(2);
/// assert_eq!(
///     query! { node props for [a, b]},
///     Query::NodeProps([a, b].into_iter().collect())
/// );
/// ```
///
/// ### S
/// To find all edges for a list of subjects:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let a = Ulid(1);
/// # let b = Ulid(2);
/// assert_eq!(
///     query! { [a, b] -?-> ? },
///     Query::S([a, b].into_iter().collect())
/// );
/// ```
///
/// ### P
/// To find all edges for a list of predicates:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let b = Ulid(2);
/// # let c = Ulid(3);
/// assert_eq!(
///     query! { ? -[b, c]-> ? },
///     Query::P([b, c].into_iter().collect())
/// );
/// ```
///
/// ### PO
/// To find all edges for all combinations of predicates and objects:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let a = Ulid(1);
/// # let b = Ulid(2);
/// # let c = Ulid(3);
/// # let d = Ulid(4);
/// assert_eq!(
///     query! { ? -[a,b]-> [c, d] },
///     Query::PO([(a, c), (a, d), (b, c), (b, d)].into_iter().collect())
/// );
/// ```
/// ### SO
/// To find all edges for all combinations of subjects and objects:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let a = Ulid(1);
/// # let b = Ulid(2);
/// # let c = Ulid(3);
/// # let d = Ulid(4);
/// assert_eq!(
///     query! { [a, b] -?-> [c, d] },
///     Query::SO([(a, c), (a, d), (b, c), (b, d)].into_iter().collect())
/// );
/// ```
///
/// ### O
/// To find all edges for a list of objects:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let a = Ulid(1);
/// # let b = Ulid(2);
/// assert_eq!(
///     query! { ? -?-> [a, b] },
///     Query::O([a, b].into_iter().collect())
/// );
/// ```
///
/// ### SP
/// To find all edges for all combinations of subjects and predicates:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let a = Ulid(1);
/// # let b = Ulid(2);
/// # let c = Ulid(3);
/// # let d = Ulid(4);
/// assert_eq!(
///     query! { [a, b] -[c, d]-> ? },
///     Query::SP([(a, c), (a, d), (b, c), (b, d)].into_iter().collect())
/// );
/// ```
///
/// ### SPO
/// To find all edges for all combinations of subjects, predicates, and objects:
///
/// ```
/// # use ulid::Ulid;
/// # use simple_triplestore::prelude::*;
/// # let a = Ulid(1);
/// # let b = Ulid(2);
/// # let c = Ulid(3);
/// # let d = Ulid(4);
/// # let e = Ulid(5);
/// # let f = Ulid(6);
/// assert_eq!(
///     query! { [a, b] -[c, d]-> [e, f] },
///     Query::SPO(
///         [
///             (a, c, e),
///             (a, c, f),
///             (a, d, e),
///             (a, d, f),
///             (b, c, e),
///             (b, c, f),
///             (b, d, e),
///             (b, d, f)
///         ]
///         .into_iter()
///         .collect()
///     )
/// );
/// ```
#[macro_export]
macro_rules! query {
    // Match specific source, edge, and destination
    (node props for $nodes:tt) => {{
        $crate::Query::NodeProps($nodes.into_iter().collect())
    }};

    ($subs:tt -?-> ?) => {{
        $crate::Query::S($subs.into_iter().collect())
    }};

    (? -$preds:tt-> ?) => {{
        $crate::Query::P($preds.into_iter().collect())
    }};

    ($subs:tt -$preds:tt-> ?) => {{
        use std::collections::HashSet;
        let mut items = HashSet::new();
        for sub in $subs {
            for pred in $preds {
                items.insert((sub, pred));
            }
        }
        $crate::Query::SP(items)
    }};

    (? -?-> $objs:tt) => {{
        $crate::Query::O($objs.into_iter().collect())
    }};

    ($subs:tt -?-> $objs:tt) => {{
        use std::collections::HashSet;
        let mut items = HashSet::new();
        for sub in $subs {
            for obj in $objs {
                items.insert((sub, obj));
            }
        }
        $crate::Query::SO(items)
    }};

    (? -$preds:tt-> $objs:tt) => {{
        use std::collections::HashSet;
        let mut items = HashSet::new();
        for sub in $preds {
            for obj in $objs {
                items.insert((sub, obj));
            }
        }
        $crate::Query::PO(items)
    }};

    ($subs:tt -$preds:tt-> $objs:tt) => {{
        use std::collections::HashSet;
        let mut items = HashSet::new();
        for sub in $subs {
            for pred in $preds {
                for obj in $objs {
                    items.insert((sub, pred, obj));
                }
            }
        }
        $crate::Query::SPO(items)
    }};
}
