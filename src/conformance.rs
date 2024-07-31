use serde::{Deserialize, Serialize};

use crate::traits::Mergeable;

pub mod extend;
pub mod insert;
pub mod iter;
pub mod merge;
pub mod query;
pub mod remove;
pub mod set;

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub(crate) struct TestMergeable {
    a: Option<String>,
    b: Option<String>,
}

impl Default for TestMergeable {
    fn default() -> Self {
        TestMergeable { a: None, b: None }
    }
}

impl Mergeable for TestMergeable {
    fn merge(&mut self, other: Self) {
        other.a.map(|a| {
            self.a = Some(a);
        });
        other.b.map(|b| {
            self.b = Some(b);
        });
    }
}
