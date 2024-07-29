use serde::{de::DeserializeOwned, Serialize};

use crate::{prelude::*, PropertiesType};

impl<
        NodeProperties: PropertiesType + Serialize + DeserializeOwned,
        EdgeProperties: PropertiesType + Serialize + DeserializeOwned,
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
