mod key_bounds;

use crate::{traits::IdType, traits::Property};

/// The three components of an edge (subject, predicate, object)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Triple<Id: IdType> {
    pub sub: Id,
    pub pred: Id,
    pub obj: Id,
}

impl<Id: IdType> PartialOrd for Triple<Id> {
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

/// A triple along with the associated NodeProps and EdgeProps.
#[derive(Debug, Clone, PartialEq)]
pub struct PropsTriple<Id: IdType, NodeProps: Property, EdgeProps: Property> {
    pub sub: (Id, NodeProps),
    pub pred: (Id, EdgeProps),
    pub obj: (Id, NodeProps),
}

impl<Id: IdType, NodeProps: Property, EdgeProps: Property>
    From<PropsTriple<Id, NodeProps, EdgeProps>> for Triple<Id>
{
    fn from(
        PropsTriple {
            sub: (sub, _),
            pred: (pred, _),
            obj: (obj, _),
        }: PropsTriple<Id, NodeProps, EdgeProps>,
    ) -> Self {
        Self { sub, pred, obj }
    }
}
