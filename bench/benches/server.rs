use std::io::BufRead;
use std::net::SocketAddr;

use criterion::{criterion_group, criterion_main, Criterion};
use criterion::{BatchSize, BenchmarkId};
use env_logger::Env;
use itertools::Itertools;
use net::quic::QuicClient;
use net::Client;
use quic_client::protocol::{ClientRequest, ClientResponse};
use rand::seq::SliceRandom;
use tokio::runtime::{self, Handle};

fn criterion_benchmark(c: &mut Criterion) {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let hlc = uhlc::HLC::default();
    let keys: Vec<String> = fake::vec![String; 2..20];
    let values: Vec<String> = fake::vec![String; 2..20];

    let rt = runtime::Runtime::new().unwrap();

    println!("Enter Port #");
    let stdin = std::io::stdin();
    let port = stdin.lock().lines().next().unwrap().unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let kvs = keys
        .into_iter()
        .cartesian_product(values.into_iter())
        .cycle()
        .take(1000)
        .map(|(k, v)| (k, v, hlc.new_timestamp()))
        .collect::<Vec<_>>();

    let connection = rt.block_on(async {
        let client = QuicClient::<ClientRequest, ClientResponse>::new().unwrap();
        let connection = client.connection(addr).await.unwrap();
        connection
    });

    c.bench_with_input(BenchmarkId::new("Put", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_batched(
                || {
                    let handle = Handle::current();
                    let _ = handle.enter();
                    let stream =
                        futures::executor::block_on(async { connection.stream().await.unwrap() });
                    let value = kvs.choose(&mut rand::thread_rng()).unwrap();
                    (stream, value)
                },
                |(mut stream, (key, value, timestamp))| async move {
                    stream
                        .request(ClientRequest::Put {
                            key: key.to_string(),
                            value: value.to_string(),
                            request_id: *timestamp,
                        })
                        .await
                        .unwrap();
                },
                BatchSize::PerIteration,
            )
    });

    c.bench_with_input(BenchmarkId::new("Get", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter_batched(
                || {
                    let handle = Handle::current();
                    let _ = handle.enter();
                    let stream =
                        futures::executor::block_on(async { connection.stream().await.unwrap() });
                    let value = kvs.choose(&mut rand::thread_rng()).unwrap();
                    (stream, value)
                },
                |(mut stream, (key, _, timestamp))| async move {
                    stream
                        .request(ClientRequest::Get {
                            key: key.to_string(),
                            request_id: *timestamp,
                        })
                        .await
                        .unwrap();
                },
                BatchSize::SmallInput,
            )
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
