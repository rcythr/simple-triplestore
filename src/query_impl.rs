use std::collections::HashSet;
use ulid::Ulid;

/// Represents a query which can be executed on a [TripleStore][crate::TripleStore].
///
/// These are most easily created using teh [query][crate::query] macro.
#[derive(Debug, PartialEq, Eq)]
pub enum Query {
    /// Fetch the NodeProperties for the given set of ids.
    NodeProps(HashSet<Ulid>),

    /// Fetch the edges for the given set of triples.
    SPO(HashSet<(Ulid, Ulid, Ulid)>),

    /// Fetch all edges which point to one of the given set of ids.
    O(HashSet<Ulid>),

    /// Fetch all edges which start at one of the given set of ids.
    S(HashSet<Ulid>),

    /// Fetch all edges which have one of the given set of edge ids.
    P(HashSet<Ulid>),

    /// Fetch all edges which have one of the given tuples as predicate and object.
    PO(HashSet<(Ulid, Ulid)>),

    /// Fetch all edges which have one of the given tuples as subject and object.
    SO(HashSet<(Ulid, Ulid)>),

    /// Fetch all edges which have one of the given tuples as subject and predicate.
    SP(HashSet<(Ulid, Ulid)>),
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
