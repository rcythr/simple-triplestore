// Marker trait for all types which are supported as TripleStore properties.
pub trait Property: Clone + std::fmt::Debug + PartialEq {}
impl<T: Clone + std::fmt::Debug + PartialEq> Property for T {}
