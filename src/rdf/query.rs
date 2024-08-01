use super::{Entity, RdfTripleStore, RdfTripleStoreError};
use crate::{
    mem::MemHashIndex,
    traits::{
        BidirIndex, Property, TripleStore, TripleStoreError, TripleStoreIter, TripleStoreQuery,
    },
    Query, QueryError,
};
use ulid::Ulid;

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStoreQuery<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    type QueryResult = RdfTripleStore<
        NodeProps,
        EdgeProps,
        MemHashIndex<String, Ulid>,
        TripleStorage::QueryResult,
    >;

    fn run(
        &self,
        query: Query<Entity>,
    ) -> Result<Self::QueryResult, QueryError<Self::Error, <<Self as TripleStoreQuery<Entity, NodeProps, EdgeProps>>::QueryResult as TripleStoreError>::Error>>{
        let mut mem_index: MemHashIndex<String, Ulid> = MemHashIndex::new();

        // Translate the query into one we can execute on the underlying graph.
        let query = query.try_map(|entity: Entity| {
            // Perform a lookup and record the result into mem_index.
            self.lookup_entity(&entity).map_err(|e| QueryError::Left(e))
        })?;

        // Execute the query on the underlying graph.
        let query_graph = self.graph.run(query).map_err(|e| match e {
            QueryError::Left(e) => QueryError::Left(RdfTripleStoreError::GraphStorageError(e)),
            QueryError::Right(e) => QueryError::Right(RdfTripleStoreError::GraphStorageError(e)),
        })?;

        // Populate the new name index with any associations we'll need.
        let vertices = query_graph
            .vertices()
            .map_err(|e| QueryError::Right(RdfTripleStoreError::GraphStorageError(e)))?;

        for id in vertices {
            if let Entity::String(s) =
                Self::lookup_id(&self.name_index, &id).map_err(|e| QueryError::Left(e))?
            {
                mem_index.set(s, id).map_err(|e| {
                    QueryError::Right(RdfTripleStoreError::NameIndexStorageError(e))
                })?;
            }
        }

        for r in query_graph.iter_edges(crate::EdgeOrder::SPO) {
            let (triple, _) =
                r.map_err(|e| QueryError::Right(RdfTripleStoreError::GraphStorageError(e)))?;

            for id in [triple.sub, triple.pred, triple.obj] {
                if let Entity::String(s) =
                    Self::lookup_id(&self.name_index, &id).map_err(|e| QueryError::Left(e))?
                {
                    mem_index.set(s, id).map_err(|e| {
                        QueryError::Right(RdfTripleStoreError::NameIndexStorageError(e))
                    })?;
                }
            }
        }

        Ok(RdfTripleStore::new(mem_index, query_graph))
    }
}
