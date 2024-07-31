/// A trait that encapsulates the error type used by other traits in the library.
pub trait TripleStoreError {
    type Error: std::fmt::Debug;
}
