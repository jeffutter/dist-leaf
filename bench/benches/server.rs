use std::io::BufRead;

use criterion::BenchmarkId;
use criterion::{criterion_group, criterion_main, Criterion};
use env_logger::Env;
use futures::Future;
use itertools::Itertools;
use quic_client::{DistKVClient, DistKVConnection, KVRequest};
use tokio::runtime;

async fn put(
    kvs: &Vec<(&String, &String)>,
    client: impl Future<Output = Result<DistKVClient, Box<(dyn std::error::Error + 'static)>>>,
) {
    let mut client = client.await.unwrap();
    for (key, value) in kvs {
        log::debug!("Putting: {} - {}", key, value);
        let request = KVRequest::Put {
            key: key.to_string(),
            value: value.to_string(),
        };
        log::debug!("Putting Req - {:?}", request);
        client.request(request).await.unwrap();
    }
}

async fn get(
    kvs: &Vec<(&String, &String)>,
    client: impl Future<Output = Result<DistKVClient, Box<(dyn std::error::Error + 'static)>>>,
) {
    let mut client = client.await.unwrap();
    for (key, _value) in kvs {
        log::debug!("Getting: {}", key);
        let request = KVRequest::Get {
            key: key.to_string(),
        };
        client.request(request).await.unwrap();
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
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

    let dist_kv_connection = rt.block_on(async { DistKVConnection::new(&port).await.unwrap() });

    c.bench_with_input(BenchmarkId::new("Put", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let dist_kv_client = dist_kv_connection.client();
                put(kvs, dist_kv_client)
            })
    });

    c.bench_with_input(BenchmarkId::new("Get", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let dist_kv_client = dist_kv_connection.client();
                get(kvs, dist_kv_client)
            })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
