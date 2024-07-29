use crate::{prelude::*, EdgeOrder, PropertyType, SetOpsError};

impl<NodeProperties: PropertyType, EdgeProperties: PropertyType>
    TripleStoreSetOps<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type SetOpsResult = MemTripleStore<NodeProperties, EdgeProperties>;
    type SetOpsResultError = ();

    fn union<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>> {
        let mut result = MemTripleStore::new();

        let (self_node_iter, self_edge_iter) = self.into_iter_nodes(EdgeOrder::SPO);
        for r in self_node_iter {
            let (id, props) = r.map_err(|e| SetOpsError::Left(e))?;
            result
                .insert_node(id, props)
                .map_err(|e| SetOpsError::Result(e))?;
        }
        for r in self_edge_iter {
            let (triple, props) = r.map_err(|e| SetOpsError::Left(e))?;
            result
                .insert_edge(triple, props)
                .map_err(|e| SetOpsError::Result(e))?;
        }

        let (other_node_iter, other_edge_iter) = other.into_iter_nodes(EdgeOrder::SPO);
        for r in other_node_iter {
            let (id, props) = r.map_err(|e| SetOpsError::Right(e))?;
            result
                .insert_node(id, props)
                .map_err(|e| SetOpsError::Result(e))?;
        }
        for r in other_edge_iter {
            let (triple, props) = r.map_err(|e| SetOpsError::Right(e))?;
            result
                .insert_edge(triple, props)
                .map_err(|e| SetOpsError::Result(e))?;
        }

        Ok(result)
    }

    fn intersection<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>> {
        let mut result: MemTripleStore<NodeProperties, EdgeProperties> = MemTripleStore::new();

        let (self_nodes, self_edges) = self.into_iter_nodes(EdgeOrder::SPO);
        let mut self_nodes = self_nodes.map(|r| r.map_err(|e| SetOpsError::Left(e)));
        let mut self_edges = self_edges.map(|r| r.map_err(|e| SetOpsError::Left(e)));

        let (other_nodes, other_edges) = other.into_iter_nodes(EdgeOrder::SPO);
        let mut other_nodes = other_nodes.map(|r| r.map_err(|e| SetOpsError::Right(e)));
        let mut other_edges = other_edges.map(|r| r.map_err(|e| SetOpsError::Right(e)));

        // Intersect nodes
        {
            let mut self_node = self_nodes.next().transpose()?;
            let mut other_node = other_nodes.next().transpose()?;

            while let (Some((left_key, left_props)), Some((right_key, _))) =
                (&self_node, &other_node)
            {
                if left_key < right_key {
                    self_node = self_nodes.next().transpose()?;
                } else if right_key < left_key {
                    other_node = other_nodes.next().transpose()?;
                } else {
                    result
                        .insert_node(*left_key, left_props.clone())
                        .map_err(|e| SetOpsError::Result(e))?;
                    self_node = self_nodes.next().transpose()?;
                    other_node = other_nodes.next().transpose()?;
                }
            }
        }

        // Intersect edges
        {
            let mut self_edge = self_edges.next().transpose()?;
            let mut other_edge = other_edges.next().transpose()?;

            while let (Some((self_key, self_props)), Some((other_key, _))) =
                (&self_edge, &other_edge)
            {
                if self_key < other_key {
                    self_edge = self_edges.next().transpose()?;
                } else if other_key < self_key {
                    other_edge = other_edges.next().transpose()?;
                } else {
                    result
                        .insert_edge(self_key.clone(), self_props.clone())
                        .map_err(|e| SetOpsError::Result(e))?;
                    self_edge = self_edges.next().transpose()?;
                    other_edge = other_edges.next().transpose()?;
                }
            }
        }
        Ok(result)
    }

    fn difference<E: std::fmt::Debug>(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<Self::SetOpsResult, SetOpsError<Self::Error, E, Self::SetOpsResultError>> {
        let mut result: MemTripleStore<NodeProperties, EdgeProperties> = MemTripleStore::new();

        let (self_nodes, self_edges) = self.into_iter_nodes(EdgeOrder::SPO);
        let mut self_nodes = self_nodes.map(|r| r.map_err(|e| SetOpsError::Left(e)));
        let mut self_edges = self_edges.map(|r| r.map_err(|e| SetOpsError::Left(e)));

        let (other_nodes, other_edges) = other.into_iter_nodes(EdgeOrder::SPO);
        let mut other_nodes = other_nodes.map(|r| r.map_err(|e| SetOpsError::Right(e)));
        let mut other_edges = other_edges.map(|r| r.map_err(|e| SetOpsError::Right(e)));

        // Intersect nodes
        {
            let mut self_node = self_nodes.next().transpose()?;
            let mut other_node = other_nodes.next().transpose()?;

            while let (Some((left_key, left_props)), Some((right_key, _))) =
                (&self_node, &other_node)
            {
                if left_key < right_key {
                    result
                        .insert_node(*left_key, left_props.clone())
                        .map_err(|e| SetOpsError::Result(e))?;
                    self_node = self_nodes.next().transpose()?;
                } else if right_key < left_key {
                    other_node = other_nodes.next().transpose()?;
                } else {
                    self_node = self_nodes.next().transpose()?;
                    other_node = other_nodes.next().transpose()?;
                }
            }

            while let Some((left_key, left_props)) = &self_node {
                result
                    .insert_node(*left_key, left_props.clone())
                    .map_err(|e| SetOpsError::Result(e))?;
                self_node = self_nodes.next().transpose()?;
            }
        }

        // Intersect edges
        {
            let mut self_edge = self_edges.next().transpose()?;
            let mut other_edge = other_edges.next().transpose()?;

            while let (Some((self_key, self_props)), Some((other_key, _))) =
                (&self_edge, &other_edge)
            {
                if self_key < other_key {
                    result
                        .insert_edge(self_key.clone(), self_props.clone())
                        .map_err(|e| SetOpsError::Result(e))?;
                    self_edge = self_edges.next().transpose()?;
                } else if other_key < self_key {
                    other_edge = other_edges.next().transpose()?;
                } else {
                    self_edge = self_edges.next().transpose()?;
                    other_edge = other_edges.next().transpose()?;
                }
            }

            while let Some((self_key, self_props)) = &self_edge {
                result
                    .insert_edge(self_key.clone(), self_props.clone())
                    .map_err(|e| SetOpsError::Result(e))?;
                self_edge = self_edges.next().transpose()?;
            }
        }
        Ok(result)
    }
}

// #[cfg(test)]
// mod test {

//     #[test]
//     fn test_union() {
//         todo!()
//     }

//     #[test]
//     fn test_intersection() {
//         todo!()
//     }

//     #[test]
//     fn test_difference() {
//         todo!()
//     }
// }
