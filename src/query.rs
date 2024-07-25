use std::collections::HashSet;
use ulid::Ulid;

#[allow(non_camel_case_types)]
pub enum Query {
    Union { left: Box<Query>, right: Box<Query> },
    Intersection { left: Box<Query>, right: Box<Query> },
    Difference { left: Box<Query>, right: Box<Query> },
    NodeProperty(HashSet<Ulid>),
    EdgeProperty(HashSet<(Ulid, Ulid, Ulid)>),
    __O(HashSet<Ulid>),
    S__(HashSet<Ulid>),
    _P_(HashSet<Ulid>),
    _PO(HashSet<(Ulid, Ulid)>),
    S_O(HashSet<(Ulid, Ulid)>),
    SP_(HashSet<(Ulid, Ulid)>),
}

impl std::ops::BitAnd for Query {
    type Output = Query;
    fn bitand(self, rhs: Self) -> Self::Output {
        Query::Intersection {
            left: Box::new(self),
            right: Box::new(rhs),
        }
    }
}

impl std::ops::BitOr for Query {
    type Output = Query;
    fn bitor(self, rhs: Self) -> Self::Output {
        Query::Union {
            left: Box::new(self),
            right: Box::new(rhs),
        }
    }
}

impl std::ops::Sub for Query {
    type Output = Query;
    fn sub(self, rhs: Self) -> Self::Output {
        Query::Difference {
            left: Box::new(self),
            right: Box::new(rhs),
        }
    }
}
