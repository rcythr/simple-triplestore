use ulid::Ulid;

mod decode;
mod decorated;
mod encode;
mod key_bounds;

pub use decorated::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Triple {
    pub sub: Ulid,
    pub pred: Ulid,
    pub obj: Ulid,
}

#[cfg(feature = "rdf")]
pub struct RdfTriple {
    pub sub: String,
    pub pred: String,
    pub obj: String,
}
