use ulid::Ulid;

use crate::{prelude::*, EdgeOrder, Triple};

struct Config {
    l1: (Ulid, String),
    l2: (Ulid, String),
    m1: (Ulid, String),
    m2: (Ulid, String),
    r1: (Ulid, String),
    r2: (Ulid, String),

    e_l1_l2: (Triple<Ulid>, String),
    e_m1_m2: (Triple<Ulid>, String),
    e_r1_r2: (Triple<Ulid>, String),
    e_r2_m2: (Triple<Ulid>, String),
    e_m2_l2: (Triple<Ulid>, String),
    e_l1_m1: (Triple<Ulid>, String),
    e_m1_r1: (Triple<Ulid>, String),
}

impl Default for Config {
    fn default() -> Self {
        let l1 = (Ulid(1), "a".into());
        let l2 = (Ulid(2), "b".into());
        let m1 = (Ulid(3), "c".into());
        let m2 = (Ulid(4), "d".into());
        let r1 = (Ulid(5), "e".into());
        let r2 = (Ulid(6), "f".into());

        let e_l1_l2 = (
            Triple {
                sub: l1.0,
                pred: Ulid(7),
                obj: l2.0,
            },
            "g".into(),
        );
        let e_m1_m2 = (
            Triple {
                sub: m1.0,
                pred: Ulid(7),
                obj: m2.0,
            },
            "h".into(),
        );
        let e_r1_r2 = (
            Triple {
                sub: r1.0,
                pred: Ulid(7),
                obj: r2.0,
            },
            "i".into(),
        );
        let e_r2_m2 = (
            Triple {
                sub: r2.0,
                pred: Ulid(8),
                obj: m2.0,
            },
            "j".into(),
        );
        let e_m2_l2 = (
            Triple {
                sub: m2.0,
                pred: Ulid(8),
                obj: l2.0,
            },
            "k".into(),
        );
        let e_l1_m1 = (
            Triple {
                sub: l1.0,
                pred: Ulid(9),
                obj: m1.0,
            },
            "l".into(),
        );
        let e_m1_r1 = (
            Triple {
                sub: m1.0,
                pred: Ulid(9),
                obj: r1.0,
            },
            "m".into(),
        );

        Self {
            l1,
            l2,
            m1,
            m2,
            r1,
            r2,
            e_l1_l2,
            e_l1_m1,
            e_m1_m2,
            e_m1_r1,
            e_m2_l2,
            e_r1_r2,
            e_r2_m2,
        }
    }
}

fn setup_left<T: TripleStore<Ulid, String, String>>(config: &Config, left: &mut T) {
    for (node, props) in [
        config.l1.clone(),
        config.l2.clone(),
        config.m1.clone(),
        config.m2.clone(),
    ] {
        left.insert_node(node, props).expect("ok");
    }

    for (triple, props) in [
        config.e_l1_l2.clone(),
        config.e_l1_m1.clone(),
        config.e_m1_m2.clone(),
        config.e_m2_l2.clone(),
    ] {
        left.insert_edge(triple, props).expect("ok");
    }
}

fn setup_right<T: TripleStore<Ulid, String, String>>(config: &Config, right: &mut T) {
    for (node, props) in [
        config.r1.clone(),
        config.r2.clone(),
        config.m1.clone(),
        config.m2.clone(),
    ] {
        right.insert_node(node, props).expect("ok");
    }

    for (triple, props) in [
        config.e_r1_r2.clone(),
        config.e_m1_r1.clone(),
        config.e_m1_m2.clone(),
        config.e_r2_m2.clone(),
    ] {
        right.insert_edge(triple, props).expect("ok");
    }
}

pub(crate) fn test_union<
    T: TripleStore<Ulid, String, String> + TripleStoreSetOps<Ulid, String, String>,
>(
    mut left: T,
    mut right: T,
) {
    let config = Config::default();
    setup_left(&config, &mut left);
    setup_right(&config, &mut right);

    let (result_nodes, result_edges) = left
        .union(right)
        .expect("ok")
        .into_iter_nodes(EdgeOrder::SPO);

    assert_eq!(
        result_nodes.map(|r| r.expect("ok")).collect::<Vec<_>>(),
        [config.l1, config.l2, config.m1, config.m2, config.r1, config.r2,].to_vec()
    );

    assert_eq!(
        result_edges.map(|r| r.expect("ok")).collect::<Vec<_>>(),
        [
            config.e_l1_l2,
            config.e_l1_m1,
            config.e_m1_m2,
            config.e_m1_r1,
            config.e_m2_l2,
            config.e_r1_r2,
            config.e_r2_m2,
        ]
        .to_vec()
    );
}

pub(crate) fn test_intersection<
    T: TripleStore<Ulid, String, String> + TripleStoreSetOps<Ulid, String, String>,
>(
    mut left: T,
    mut right: T,
) {
    let config = Config::default();
    setup_left(&config, &mut left);
    setup_right(&config, &mut right);

    let (result_nodes, result_edges) = left
        .intersection(right)
        .expect("ok")
        .into_iter_nodes(EdgeOrder::SPO);

    assert_eq!(
        result_nodes.map(|r| r.expect("ok")).collect::<Vec<_>>(),
        [config.m1, config.m2].to_vec()
    );

    assert_eq!(
        result_edges.map(|r| r.expect("ok")).collect::<Vec<_>>(),
        [config.e_m1_m2,].to_vec()
    );
}

pub(crate) fn test_difference<
    T: TripleStore<Ulid, String, String> + TripleStoreSetOps<Ulid, String, String>,
>(
    mut left: T,
    mut right: T,
) {
    let config = Config::default();
    setup_left(&config, &mut left);
    setup_right(&config, &mut right);

    let (result_nodes, result_edges) = left
        .difference(right)
        .expect("ok")
        .into_iter_nodes(EdgeOrder::SPO);

    assert_eq!(
        result_nodes.map(|r| r.expect("ok")).collect::<Vec<_>>(),
        [config.l1, config.l2].to_vec()
    );

    assert_eq!(
        result_edges.map(|r| r.expect("ok")).collect::<Vec<_>>(),
        [config.e_l1_l2,].to_vec()
    );
}
