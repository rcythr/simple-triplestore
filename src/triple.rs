use ulid::Ulid;

mod decorated;
pub use decorated::*;

mod decode;
mod encode;

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
