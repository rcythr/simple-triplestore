use super::ConcreteIdType;

pub trait IdGenerator<Id: ConcreteIdType> {
    fn clone(&self) -> Box<dyn IdGenerator<Id>>;
    fn fresh(&mut self) -> Id;
}
