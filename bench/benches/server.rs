use std::io::BufRead;

use criterion::BenchmarkId;
use criterion::{criterion_group, criterion_main, Criterion};
use env_logger::Env;
use itertools::Itertools;
use quic_client::DistKVClient;
use tokio::runtime;

async fn put(kvs: &Vec<(&String, &String)>, mut client: DistKVClient) {
    for (key, value) in kvs {
        log::debug!("Putting: {} - {}", key, value);
        let request = net::KVRequestType::Put(key.to_string(), value.to_string());
        log::debug!("Putting Req - {:?}", request);
        client.request(request).await.unwrap();
    }
}

async fn get(kvs: &Vec<(&String, &String)>, mut client: DistKVClient) {
    for (key, _value) in kvs {
        log::debug!("Getting: {}", key);
        let request = net::KVRequestType::Get(key.to_string());
        client.request(request).await.unwrap();
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    env_logger::Builder::from_env(Env::default().default_filter_or(
        "debug,netlink_proto=info,libp2p_ping=info,libp2p_swarm=info,libp2p_tcp=info,libp2p_mdns=info,libp2p_dns=info,yamux=info,multistream_select=info",
    ))
    .init();
    let keys: Vec<String> = fake::vec![String; 2..10];
    let values: Vec<String> = fake::vec![String; 2..10];

    let kvs = keys
        .iter()
        .cartesian_product(values.iter())
        .cycle()
        .take(100)
        .collect_vec();

    let rt = runtime::Runtime::new().unwrap();

    println!("Enter Port #");
    let stdin = std::io::stdin();
    let port = stdin.lock().lines().next().unwrap().unwrap();

    let dist_kv_client = rt.block_on(async { DistKVClient::new(&port).await.unwrap() });

    c.bench_with_input(BenchmarkId::new("Put", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                // let dist_kv_client = rt.block_on(async { DistKVClient::new(&port).await.unwrap() });
                // let dist_kv_client = rt.block_on(async { DistKVClient::new(&port).await.unwrap() });
                put(kvs, dist_kv_client.clone())
            })
    });

    c.bench_with_input(BenchmarkId::new("Get", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                // let dist_kv_client = rt.block_on(async { DistKVClient::new(&port).await.unwrap() });
                get(kvs, dist_kv_client.clone())
            })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
