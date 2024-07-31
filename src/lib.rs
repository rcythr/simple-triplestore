//! A [triplestore](https://en.wikipedia.org/wiki/Triplestore) implementation which can be used as a flexible graph database with support for custom node and edge properties.
//!
//! ## Data Model
//! Each vertex and edge (collectively called `nodes`) are associated with an id (i.e. `u64` or [Ulid](https://docs.rs/ulid/latest/ulid/struct.Ulid.html)).
//!
//! Property data is stored as
//!   * `Id -> NodeProps`
//!   * `Id -> EdgeProps`.
//!
//! Graph relationships are stored three times as <code>(Id, Id, Id) -> Id</code> with the following sort orders:
//!   * Subject, Predicate, Object
//!   * Predicate, Object, Subject
//!   * Object, Subject, Predicate
//!
//! This allows for any graph query to be decomposed into a range query on the lookup with the ideal ordering. For example,
//!
//! * `query!{ a -b-> ? }` becomes a query on the subject-predicate-object table.
//! * `query!{ ? -a-> b }` becomes a query on the position-object-subject table.
//! * `query!{ a -?-> b }` becomes a query on the object-subject-position table.
//!
//! ## Supported Key-Value Backends
//!   * [Memory](https://docs.rs/simple-triplestore/latest/simple_triplestore/struct.MemTripleStore.html)
//!   * [Sled](https://docs.rs/simple-triplestore/latest/simple_triplestore/struct.SledTripleStore.html) ( with the `sled` feature )

use std::collections::HashSet;

pub mod id;
pub mod mem;
pub mod prelude;
#[cfg(feature = "sled")]
pub mod sled;

#[cfg(test)]
mod conformance;
pub mod traits;
pub mod triple;

pub use crate::{
    id::ulid::UlidIdGenerator,
    mem::MemTripleStore,
    traits::{ExtendError, IdGenerator, MergeError, Mergeable, QueryError, SetOpsError},
    triple::{PropsTriple, Triple},
};

#[cfg(feature = "sled")]
pub use crate::sled::{SledTripleStore, SledTripleStoreError};

/// The order for edges which should be returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeOrder {
    /// Subject, Predicate, Object
    SPO,

    /// Predicate, Object, Subject,
    POS,

    /// Object, Subject, Predicate
    OSP,
}

impl Default for EdgeOrder {
    fn default() -> Self {
        Self::SPO
    }
}

/// Represents a query which can be executed on a [TripleStore][crate::TripleStore].
///
/// These are most easily created using teh [query][crate::query] macro.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Query<Id: traits::IdType> {
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

impl<Id: traits::IdType, I: IntoIterator<Item = Triple<Id>>> From<I> for Query<Id> {
    fn from(value: I) -> Self {
        Query::SPO(value.into_iter().map(|t| (t.sub, t.pred, t.obj)).collect())
    }
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
