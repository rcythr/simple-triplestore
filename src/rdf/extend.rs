use super::{Entity, RdfTripleStore};
use crate::{
    traits::{BidirIndex, Property, TripleStore, TripleStoreExtend, TripleStoreInsert},
    EdgeOrder, ExtendError,
};
use ulid::Ulid;

impl<
        NodeProps: Property,
        EdgeProps: Property,
        NameIndex: BidirIndex<Left = String, Right = Ulid>,
        TripleStorage: TripleStore<Ulid, NodeProps, EdgeProps>,
    > TripleStoreExtend<Entity, NodeProps, EdgeProps>
    for RdfTripleStore<NodeProps, EdgeProps, NameIndex, TripleStorage>
{
    fn extend<E: std::fmt::Debug>(
        &mut self,
        other: impl TripleStore<Entity, NodeProps, EdgeProps, Error = E>,
    ) -> Result<(), ExtendError<Self::Error, E>> {
        let (other_nodes, other_edges) = other.into_iter_nodes(EdgeOrder::SPO);

        for r in other_nodes {
            let (id, props) = r.map_err(|e| ExtendError::Right(e))?;

            self.insert_node(id, props)
                .map_err(|e| ExtendError::Left(e))?;
        }

        for r in other_edges {
            let (triple, props) = r.map_err(|e| ExtendError::Right(e))?;

            self.insert_edge(triple, props)
                .map_err(|e| ExtendError::Left(e))?;
        }

        Ok(())
    }
}
