use itertools::Itertools;

use crate::{
    prelude::*,
    traits::{IdType, Property},
    EdgeOrder,
};

/// Wrapper type for errors produced by either self or other in [TripleStore::try_eq()]
#[derive(Debug)]
pub enum TryEqError<LeftError: std::fmt::Debug, RightError: std::fmt::Debug> {
    Left(LeftError),
    Right(RightError),
}

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
    ) -> Result<bool, TryEqError<Self::Error, OError>> {
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
