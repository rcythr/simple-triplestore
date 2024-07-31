use crate::{prelude::*, IdType, Property};

#[derive(Debug)]
pub enum QueryError<SourceError: std::fmt::Debug, ResultError: std::fmt::Debug> {
    Left(SourceError),
    Right(ResultError),
}

/// A trait for querying operations in a [TripleStore].
///
/// Supports arbitrary source, predicate, and object queries, as well as lookups for properties of nodes and edges.
pub trait TripleStoreQuery<Id: IdType, NodeProps: Property, EdgeProps: Property>:
    TripleStoreError
{
    /// The result type of a query.
    type QueryResult: TripleStore<Id, NodeProps, EdgeProps>;
    type QueryResultError: std::fmt::Debug;

    /// Execute a query and return the result.
    fn run(
        &self,
        query: Query<Id>,
    ) -> Result<Self::QueryResult, QueryError<Self::Error, Self::QueryResultError>>;
}
