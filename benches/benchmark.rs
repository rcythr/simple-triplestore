use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use rand::seq::SliceRandom;
use rand::thread_rng;
use simple_triplestore::{
    prelude::*, MemTripleStore, Query, SledTripleStore, Triple, UlidIdGenerator,
};
use ulid::Ulid;

fn bench_insert_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_node");
    for n in [1000, 100000, 1000000] {
        let tempdir = tempdir::TempDir::new("sled").unwrap();
        let db = sled::open(tempdir.path()).unwrap();

        let mut mem: MemTripleStore<Ulid, [u8; 32], ()> =
            MemTripleStore::new(UlidIdGenerator::new());
        let mut sled: SledTripleStore<Ulid, [u8; 32], ()> =
            SledTripleStore::new(&db, UlidIdGenerator::new()).unwrap();

        // Preload some elements.
        for _ in 0..n {
            let id = Ulid::new();
            mem.insert_node(id, [0u8; 32]).unwrap();
            sled.insert_node(id, [0u8; 32]).unwrap();
        }

        group.bench_function(BenchmarkId::new("MemTripleStore", n), |b| {
            b.iter(|| {
                let id = Ulid::new();
                mem.insert_node(id, [0u8; 32]).unwrap();
            })
        });

        group.bench_function(BenchmarkId::new("SledTripleStore", n), |b| {
            b.iter(|| {
                let id = Ulid::new();
                sled.insert_node(id, [0u8; 32]).unwrap();
            })
        });
    }
}

fn bench_insert_edge(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_edge");
    for n in [1000, 100000, 1000000] {
        let tempdir = tempdir::TempDir::new("sled").unwrap();
        let db = sled::open(tempdir.path()).unwrap();

        let mut mem: MemTripleStore<Ulid, [u8; 32], [u8; 32]> =
            MemTripleStore::new(UlidIdGenerator::new());
        let mut sled: SledTripleStore<Ulid, [u8; 32], [u8; 32]> =
            SledTripleStore::new(&db, UlidIdGenerator::new()).unwrap();

        // Preload some elements.
        for _ in 0..n {
            let sub = Ulid::new();
            let pred = Ulid::new();
            let obj = Ulid::new();
            mem.insert_edge(Triple { sub, pred, obj }, [0u8; 32])
                .unwrap();
            sled.insert_edge(Triple { sub, pred, obj }, [0u8; 32])
                .unwrap();
        }

        group.bench_function(BenchmarkId::new("MemTripleStore", n), |b| {
            b.iter(|| {
                let sub = Ulid::new();
                let pred = Ulid::new();
                let obj = Ulid::new();
                mem.insert_edge(Triple { sub, pred, obj }, [0u8; 32])
                    .unwrap();
            })
        });

        group.bench_function(BenchmarkId::new("SledTripleStore", n), |b| {
            b.iter(|| {
                let sub = Ulid::new();
                let pred = Ulid::new();
                let obj = Ulid::new();
                sled.insert_edge(Triple { sub, pred, obj }, [0u8; 32])
                    .unwrap();
            })
        });
    }
}

fn bench_query_node(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_node");
    for n in [1000, 100000, 1000000] {
        let tempdir = tempdir::TempDir::new("sled").unwrap();
        let db = sled::open(tempdir.path()).unwrap();

        let mut mem: MemTripleStore<Ulid, [u8; 32], ()> =
            MemTripleStore::new(UlidIdGenerator::new());
        let mut sled: SledTripleStore<Ulid, [u8; 32], ()> =
            SledTripleStore::new(&db, UlidIdGenerator::new()).unwrap();

        // Preload some elements.
        let mut ids = Vec::new();
        for _ in 0..n {
            let id = Ulid::new();
            mem.insert_node(id, [0u8; 32]).unwrap();
            sled.insert_node(id, [0u8; 32]).unwrap();
            ids.push(id);
        }
        ids.shuffle(&mut thread_rng());
        let ids = ids.into_iter().take(1000).collect::<Vec<_>>();

        group.throughput(criterion::Throughput::Elements(1000));
        group.bench_function(BenchmarkId::new("MemTripleStore", n), |b| {
            b.iter(|| black_box(mem.run(query! {node props for {ids.clone()}}).unwrap()));
        });

        group.bench_function(BenchmarkId::new("SledTripleStore", n), |b| {
            b.iter(|| black_box(sled.run(query! {node props for {ids.clone()}}).unwrap()));
        });
    }
}

fn bench_query_edge(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_edge");
    for n in [1000, 100000, 1000000] {
        let tempdir = tempdir::TempDir::new("sled").unwrap();
        let db = sled::open(tempdir.path()).unwrap();

        let mut mem: MemTripleStore<Ulid, (), Ulid> = MemTripleStore::new(UlidIdGenerator::new());
        let mut sled: SledTripleStore<Ulid, (), Ulid> =
            SledTripleStore::new(&db, UlidIdGenerator::new()).unwrap();

        // Preload some elements.
        let mut triples = Vec::new();
        for _ in 0..n {
            let sub = Ulid::new();
            let pred = Ulid::new();
            let obj = Ulid::new();
            let triple = Triple { sub, pred, obj };
            mem.insert_edge(triple.clone(), pred).unwrap();
            sled.insert_edge(triple.clone(), pred).unwrap();
            triples.push(triple);
        }
        triples.shuffle(&mut thread_rng());
        let triples: Query<Ulid> = triples.into_iter().take(1000).into();

        group.throughput(criterion::Throughput::Elements(1000));
        group.bench_function(BenchmarkId::new("MemTripleStore", n), |b| {
            b.iter(|| black_box(mem.run(triples.clone()).unwrap()))
        });

        group.bench_function(BenchmarkId::new("SledTripleStore", n), |b| {
            b.iter(|| black_box(sled.run(triples.clone()).unwrap()))
        });
    }
}

criterion_group!(
    benches,
    bench_insert_node,
    bench_insert_edge,
    bench_query_node,
    bench_query_edge
);
criterion_main!(benches);
