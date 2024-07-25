/// Types which can be merged together in a sensible fashion.
///
/// Intended to be used for hashtables or json objects.
pub trait Mergeable {
    ///
    fn merge(&mut self, other: Self);
}

impl Mergeable for () {
    fn merge(&mut self, _other: Self) {}
}
