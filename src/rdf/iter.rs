use std::sync::Arc;

use super::{Entity, RdfTripleStore, RdfTripleStoreError};
use crate::{
    traits::{BidirIndex, Property, TripleStore, TripleStoreIntoIter, TripleStoreIter},
    EdgeOrder, PropsTriple, Triple,
};
use ulid::Ulid;

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStoreIter<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    fn vertices(&self) -> Result<impl Iterator<Item = Entity>, Self::Error> {
        Ok(self
            .graph
            .vertices()
            .map_err(|e| super::RdfTripleStoreError::GraphStorageError(e))?
            .map(|id| Self::lookup_id(&self.name_index, &id))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter())
    }

    fn iter_nodes(
        &self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Entity, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Entity>, EdgeProps), Self::Error>>,
    ) {
        let (iter_vertices, iter_edges) = self.graph.iter_nodes(order);

        let iter_vertices = iter_vertices.map(|r| match r {
            Ok((id, node_props)) => Ok((Self::lookup_id(&self.name_index, &id)?, node_props)),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        });

        let iter_edges = iter_edges.map(|r| match r {
            Ok((triple, edge_props)) => Ok((
                triple.try_map(|id| Self::lookup_id(&self.name_index, &id))?,
                edge_props,
            )),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        });

        (iter_vertices, iter_edges)
    }

    fn iter_vertices<'a>(
        &'a self,
    ) -> impl Iterator<Item = Result<(Entity, NodeProps), Self::Error>> + 'a {
        self.graph.iter_vertices().map(|r| match r {
            Ok((id, node_props)) => Ok((Self::lookup_id(&self.name_index, &id)?, node_props)),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        })
    }

    fn iter_edges_with_props<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Entity, NodeProps, EdgeProps>, Self::Error>> + 'a
    {
        self.graph.iter_edges_with_props(order).map(|r| match r {
            Ok(triple) => Ok(triple.try_map(
                |(id, node_props)| Ok((Self::lookup_id(&self.name_index, &id)?, node_props)),
                |(id, edge_props)| Ok((Self::lookup_id(&self.name_index, &id)?, edge_props)),
            )?),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        })
    }

    fn iter_edges<'a>(
        &'a self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Entity>, EdgeProps), Self::Error>> + 'a {
        self.graph.iter_edges(order).map(|r| match r {
            Ok((triple, edge_props)) => Ok((
                triple.try_map(|id| Self::lookup_id(&self.name_index, &id))?,
                edge_props,
            )),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        })
    }
}

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStoreIntoIter<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    fn into_iter_nodes(
        self,
        order: EdgeOrder,
    ) -> (
        impl Iterator<Item = Result<(Entity, NodeProps), Self::Error>>,
        impl Iterator<Item = Result<(Triple<Entity>, EdgeProps), Self::Error>>,
    ) {
        let (iter_vertices, iter_edges) = self.graph.into_iter_nodes(order);

        let name_index_vertices = Arc::new(self.name_index);
        let name_index_edges = name_index_vertices.clone();

        let iter_vertices = iter_vertices.map(move |r| match r {
            Ok((id, node_props)) => Ok((
                Self::lookup_id(name_index_vertices.as_ref(), &id)?,
                node_props,
            )),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        });

        let iter_edges = iter_edges.map(move |r| match r {
            Ok((triple, edge_props)) => Ok((
                triple.try_map(|id| Self::lookup_id(name_index_edges.as_ref(), &id))?,
                edge_props,
            )),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        });

        (iter_vertices, iter_edges)
    }

    fn into_iter_vertices(self) -> impl Iterator<Item = Result<(Entity, NodeProps), Self::Error>> {
        let name_index = self.name_index;
        self.graph.into_iter_vertices().map(move |r| match r {
            Ok((id, node_props)) => Ok((Self::lookup_id(&name_index, &id)?, node_props)),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        })
    }

    fn into_iter_edges_with_props(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<PropsTriple<Entity, NodeProps, EdgeProps>, Self::Error>> {
        let name_index = self.name_index;
        self.graph
            .into_iter_edges_with_props(order)
            .map(move |r| match r {
                Ok(triple) => Ok(triple.try_map(
                    |(id, node_props)| Ok((Self::lookup_id(&name_index, &id)?, node_props)),
                    |(id, edge_props)| Ok((Self::lookup_id(&name_index, &id)?, edge_props)),
                )?),
                Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
            })
    }

    fn into_iter_edges(
        self,
        order: EdgeOrder,
    ) -> impl Iterator<Item = Result<(Triple<Entity>, EdgeProps), Self::Error>> {
        let name_index = self.name_index;
        self.graph.into_iter_edges(order).map(move |r| match r {
            Ok((triple, edge_props)) => Ok((
                triple.try_map(|id| Self::lookup_id(&name_index, &id))?,
                edge_props,
            )),
            Err(e) => Err(RdfTripleStoreError::GraphStorageError(e)),
        })
    }
}
