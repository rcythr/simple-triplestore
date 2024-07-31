use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use simple_triplestore::prelude::*;
use ulid::Ulid;

fn bench_insert_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_node");
    for n in [1000, 100000, 1000000] {
        let tempdir = tempdir::TempDir::new("sled").unwrap();
        let db = sled::open(tempdir.path()).unwrap();

        let mut mem: MemTripleStore<Ulid, Ulid, ()> = MemTripleStore::new(UlidIdGenerator::new());
        let mut sled: SledTripleStore<Ulid, Ulid, ()> =
            SledTripleStore::new(&db, UlidIdGenerator::new()).unwrap();

        // Preload some elements.
        for _ in 0..n {
            let id = Ulid::new();
            mem.insert_node(id, id).unwrap();
            sled.insert_node(id, id).unwrap();
        }

        group.bench_function(BenchmarkId::new("MemTripleStore", n), |b| {
            b.iter(|| {
                let id = Ulid::new();
                mem.insert_node(id, id).unwrap();
            })
        });

        group.bench_function(BenchmarkId::new("SledTripleStore", n), |b| {
            b.iter(|| {
                let id = Ulid::new();
                sled.insert_node(id, id).unwrap();
            })
        });
    }
}

fn bench_insert_edge(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_edge");
    for n in [1000, 100000, 1000000] {
        let tempdir = tempdir::TempDir::new("sled").unwrap();
        let db = sled::open(tempdir.path()).unwrap();

        let mut mem: MemTripleStore<Ulid, (), Ulid> = MemTripleStore::new(UlidIdGenerator::new());
        let mut sled: SledTripleStore<Ulid, (), Ulid> =
            SledTripleStore::new(&db, UlidIdGenerator::new()).unwrap();

        // Preload some elements.
        for _ in 0..n {
            let sub = Ulid::new();
            let pred = Ulid::new();
            let obj = Ulid::new();
            mem.insert_edge(Triple { sub, pred, obj }, pred).unwrap();
            sled.insert_edge(Triple { sub, pred, obj }, pred).unwrap();
        }

        group.bench_function(BenchmarkId::new("MemTripleStore", n), |b| {
            b.iter(|| {
                let sub = Ulid::new();
                let pred = Ulid::new();
                let obj = Ulid::new();
                mem.insert_edge(Triple { sub, pred, obj }, pred).unwrap();
            })
        });

        group.bench_function(BenchmarkId::new("SledTripleStore", n), |b| {
            b.iter(|| {
                let sub = Ulid::new();
                let pred = Ulid::new();
                let obj = Ulid::new();
                sled.insert_edge(Triple { sub, pred, obj }, pred).unwrap();
            })
        });
    }
}

criterion_group!(benches, bench_insert_node, bench_insert_edge);
criterion_main!(benches);
