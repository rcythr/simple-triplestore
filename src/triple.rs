use ulid::Ulid;

mod decode;
mod encode;
mod key_bounds;

use crate::PropertyType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Triple {
    pub sub: Ulid,
    pub pred: Ulid,
    pub obj: Ulid,
}

impl PartialOrd for Triple {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.sub.partial_cmp(&other.sub) {
            None => None,
            Some(std::cmp::Ordering::Less) => Some(std::cmp::Ordering::Less),
            Some(std::cmp::Ordering::Greater) => Some(std::cmp::Ordering::Greater),
            Some(std::cmp::Ordering::Equal) => match self.pred.partial_cmp(&other.pred) {
                None => None,
                Some(std::cmp::Ordering::Less) => Some(std::cmp::Ordering::Less),
                Some(std::cmp::Ordering::Greater) => Some(std::cmp::Ordering::Greater),
                Some(std::cmp::Ordering::Equal) => match self.obj.partial_cmp(&other.obj) {
                    None => None,
                    Some(std::cmp::Ordering::Less) => Some(std::cmp::Ordering::Less),
                    Some(std::cmp::Ordering::Greater) => Some(std::cmp::Ordering::Greater),
                    Some(std::cmp::Ordering::Equal) => Some(std::cmp::Ordering::Equal),
                },
            },
        }
    }
}

// A triple along with the associated Node and Edge properties for .
#[derive(Debug, Clone, PartialEq)]
pub struct PropsTriple<NodeProperties: PropertyType, EdgeProperties: PropertyType> {
    pub sub: (Ulid, NodeProperties),
    pub pred: (Ulid, EdgeProperties),
    pub obj: (Ulid, NodeProperties),
}

impl<NodeProperties: PropertyType, EdgeProperties: PropertyType>
    From<PropsTriple<NodeProperties, EdgeProperties>> for Triple
{
    fn from(
        PropsTriple {
            sub: (sub, _),
            pred: (pred, _),
            obj: (obj, _),
        }: PropsTriple<NodeProperties, EdgeProperties>,
    ) -> Self {
        Self { sub, pred, obj }
    }
}
