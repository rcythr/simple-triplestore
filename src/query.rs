use std::collections::HashSet;
use ulid::Ulid;

#[allow(non_camel_case_types)]
pub enum Query {
    NodeProperty(HashSet<Ulid>),
    EdgeProperty(HashSet<(Ulid, Ulid, Ulid)>),
    __O(HashSet<Ulid>),
    S__(HashSet<Ulid>),
    _P_(HashSet<Ulid>),
    _PO(HashSet<(Ulid, Ulid)>),
    S_O(HashSet<(Ulid, Ulid)>),
    SP_(HashSet<(Ulid, Ulid)>),
}
