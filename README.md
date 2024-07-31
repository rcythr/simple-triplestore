# simple-triplestore &emsp; [![Latest Docs]][docs.rs] [![Latest Crate]][crates.io]
[Latest Docs]: https://img.shields.io/docsrs/simple-triplestore/0.0.2?label=docs
[Latest Crate]: https://img.shields.io/crates/v/simple-triplestore
[docs.rs]: https://docs.rs/simple-triplestore/latest/
[crates.io]: https://crates.io/crates/simple-triplestore

A [triplestore](https://en.wikipedia.org/wiki/Triplestore) implementation which can be used as a flexible graph database with support for custom node and edge properties.

## Data Model
Each vertex and edge (collectively called `nodes`) are associated with an id (i.e. `u64` or [Ulid](https://docs.rs/ulid/latest/ulid/struct.Ulid.html)). 

Property data is stored as 
  * `Id -> NodeProps`
  * `Id -> EdgeProps`.

Graph relationships are stored three times as <code>(Id, Id, Id) -> Id</code> with the following sort orders:
  * Subject, Predicate, Object
  * Predicate, Object, Subject
  * Object, Subject, Predicate

This allows for any graph query to be decomposed into a range query on the lookup with the ideal ordering. For example,

* `query!{ a -b-> ? }` becomes a query on the subject-predicate-object table.
* `query!{ ? -a-> b }` becomes a query on the position-object-subject table.
* `query!{ a -?-> b }` becomes a query on the object-subject-position table.

## Supported Key-Value Backends
  * [Memory](https://docs.rs/simple-triplestore/latest/simple_triplestore/struct.MemTripleStore.html)
  * [Sled](https://docs.rs/simple-triplestore/latest/simple_triplestore/struct.SledTripleStore.html) ( with the `sled` feature )

## Example

Pull in various includes we need:
```rust
use ulid::Ulid;
use simple_triplestore::prelude::*;
let mut db = MemTripleStore::new(UlidIdGenerator::new());
```

Get some identifiers. In real applications these will come from an index or another lookup table.
```rust
let node_1 = Ulid(123);
let node_2 = Ulid(456);
let node_3 = Ulid(789);
let edge = Ulid(999);
```

Insert nodes and edges with user-defined property types. For a given TripleStore we can have one type for Nodes and one for Edges.
```
db.insert_node(node_1, "foo".to_string())?;
db.insert_node(node_2, "bar".to_string())?;
db.insert_node(node_3, "baz".to_string())?;
db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_2}, Vec::from([1,2,3]))?;
db.insert_edge(Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6]))?;
```

We can now query for edges which end at `node_3`, and find that there is only one.

```rust
assert_eq!(
  db.run(query!{ ? -?-> [node_3] })?
    .iter_edges(EdgeOrder::default())
    .map(|r| r.expect("ok"))
    .collect::<Vec<_>>(),
  [
    (Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6])),
  ]
);
```

We can also query for all edges which have the predicate `edge`, and find both of the edges we added:
```rust
assert_eq!(
  db.run(query!{ ? -[edge]-> ? })?
    .iter_edges(EdgeOrder::default())
    .map(|r| r.expect("ok"))
    .collect::<Vec<_>>(),
  [
    (Triple{sub: node_1, pred: edge, obj: node_2}, Vec::from([1,2,3])),
    (Triple{sub: node_1, pred: edge, obj: node_3}, Vec::from([4,5,6])),
  ]
);
```
