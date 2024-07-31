use super::IdType;

pub trait IdGenerator<Id: IdType> {
    fn clone(&self) -> Box<dyn IdGenerator<Id>>;
    fn fresh(&mut self) -> Id;
}
