pub trait IndexType: std::fmt::Debug + std::hash::Hash + Eq + Clone {}
impl<T: std::fmt::Debug + std::hash::Hash + Eq + Clone> IndexType for T {}
