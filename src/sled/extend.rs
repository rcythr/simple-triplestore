use serde::{de::DeserializeOwned, Serialize};

use crate::{prelude::*, PropertyType};

impl<
        NodeProperties: PropertyType + Serialize + DeserializeOwned,
        EdgeProperties: PropertyType + Serialize + DeserializeOwned,
    > TripleStoreExtend<NodeProperties, EdgeProperties>
    for SledTripleStore<NodeProperties, EdgeProperties>
{
    fn extend<E: std::fmt::Debug>(
        &mut self,
        _other: impl TripleStore<NodeProperties, EdgeProperties, Error = E>,
    ) -> Result<(), ExtendError<Self::Error, E>> {
        todo!();
    }
}

// #[cfg(test)]
// mod test {
//     #[test]
//     fn test_extend() {
//         todo!()
//     }
// }
