use std::collections::HashSet;
use ulid::Ulid;

use crate::Triple;

#[allow(non_camel_case_types)]
pub enum Query {
    NodeProps(HashSet<Ulid>),
    EdgeProps(HashSet<Triple>),
    O(HashSet<Ulid>),
    S(HashSet<Ulid>),
    P(HashSet<Ulid>),
    PO(HashSet<(Ulid, Ulid)>),
    SO(HashSet<(Ulid, Ulid)>),
    SP(HashSet<(Ulid, Ulid)>),
}
