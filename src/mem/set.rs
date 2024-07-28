use crate::{prelude::*, PropertiesType};

impl<NodeProperties: PropertiesType, EdgeProperties: PropertiesType>
    TripleStoreSetOps<NodeProperties, EdgeProperties>
    for MemTripleStore<NodeProperties, EdgeProperties>
{
    type SetOpsResult = MemTripleStore<NodeProperties, EdgeProperties>;

    fn union(
        self,
        other: impl TripleStoreIntoIter<NodeProperties, EdgeProperties>,
    ) -> Result<Self::SetOpsResult, Self::Error> {
        let mut result = MemTripleStore::new();

        let (self_node_iter, self_edge_iter) = self.into_iters();
        for r in self_node_iter {
            let (id, props) = r?;
            result.insert_node(id, props)?;
        }
        for r in self_edge_iter {
            let (triple, props) = r?;
            result.insert_edge(triple, props)?;
        }

        let (other_node_iter, other_edge_iter) = other.into_iters();
        for r in other_node_iter {
            let (id, props) = r.map_err(|_| ())?; // TOOD: Replace mem::Error with a real error so we can capture this.
            result.insert_node(id, props)?;
        }
        for r in other_edge_iter {
            let (triple, props) = r.map_err(|_| ())?;
            result.insert_edge(triple, props)?;
        }

        Ok(result)
    }

    fn intersection(self, other: Self) -> Result<Self::SetOpsResult, Self::Error> {
        let mut result = MemTripleStore::new();

        // Intersect nodes
        for (node, data) in self.node_props {
            if let Some(_) = other.node_props.get(&node) {
                result.node_props.insert(node, data);
            }
        }

        let edge_data = self.edge_props;

        // Intersect edges
        let mut left_iter = self.spo_data.into_iter();
        let mut left = left_iter.next();

        let mut right_iter = other.spo_data.into_iter();
        let mut right = right_iter.next();

        while left.is_some() && right.is_some() {
            let left_key = left.as_ref().unwrap().0;
            let right_key = right.as_ref().unwrap().0;

            if left_key < right_key {
                left = left_iter.next();
            } else if right_key < left_key {
                right = right_iter.next();
            } else {
                let triple = Triple::decode_spo(&left_key);
                if result.node_props.contains_key(&triple.sub)
                    && result.node_props.contains_key(&triple.obj)
                {
                    if let Some(data) = edge_data.get(&left.as_ref().unwrap().1) {
                        result.insert_edge(triple, data.clone())?;
                    }
                }
                left = left_iter.next();
                right = right_iter.next();
            }
        }

        Ok(result)
    }

    fn difference(self, other: Self) -> Result<Self::SetOpsResult, Self::Error> {
        let mut result = MemTripleStore::new();

        // Intersect nodes
        result.node_props = self.node_props.clone();
        for (node, _) in other.node_props {
            if let Some(_) = self.node_props.get(&node) {
                result.node_props.remove(&node);
            }
        }

        let edge_data = self.edge_props;

        // Intersect edges
        let mut left_iter = self.spo_data.into_iter();
        let mut left = left_iter.next();

        let mut right_iter = other.spo_data.into_iter();
        let mut right = right_iter.next();

        while left.is_some() && right.is_some() {
            let left_key = left.as_ref().unwrap().0;
            let right_key = right.as_ref().unwrap().0;

            if left_key < right_key {
                let triple = Triple::decode_spo(&left_key);

                if let Some(data) = edge_data.get(&left.as_ref().unwrap().1) {
                    result.insert_edge(triple, data.clone())?;
                }

                left = left_iter.next();
            } else if right_key < left_key {
                right = right_iter.next();
            } else {
                left = left_iter.next();
                right = right_iter.next();
            }
        }

        while left.is_some() {
            let triple = Triple::decode_spo(&left.as_ref().unwrap().0);
            if let Some(data) = edge_data.get(&left.as_ref().unwrap().1) {
                result.insert_edge(triple, data.clone())?;
            }
            left = left_iter.next();
        }

        Ok(result)
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_union() {
        todo!()
    }

    #[test]
    fn test_intersection() {
        todo!()
    }

    #[test]
    fn test_difference() {
        todo!()
    }
}
