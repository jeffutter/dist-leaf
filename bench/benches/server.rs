use std::io::BufRead;
use std::net::SocketAddr;

use criterion::BenchmarkId;
use criterion::{criterion_group, criterion_main, Criterion};
use env_logger::Env;
use itertools::Itertools;
use quic_client::protocol::{ClientRequest, ClientResponse};
use quic_transport::quic;
use quic_transport::MessageClient;
use tokio::runtime;

fn criterion_benchmark(c: &mut Criterion) {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let hlc = uhlc::HLC::default();
    let keys: Vec<String> = fake::vec![String; 2..10];
    let values: Vec<String> = fake::vec![String; 2..10];

    let kvs = keys
        .iter()
        .cartesian_product(values.iter())
        .cycle()
        .take(100)
        .map(|(k, v)| (k, v, hlc.new_timestamp()))
        .collect_vec();

    let rt = runtime::Runtime::new().unwrap();

    println!("Enter Port #");
    let stdin = std::io::stdin();
    let port = stdin.lock().lines().next().unwrap().unwrap();
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let connection = rt.block_on(async {
        let client = quic::Client::<ClientRequest, ClientResponse>::new().unwrap();
        client.connect(addr).await.unwrap()
    });

    c.bench_with_input(BenchmarkId::new("Put", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let connection = connection.clone();
                async move {
                    let mut stream = connection.stream().await.unwrap();
                    for (key, value, timestamp) in kvs {
                        stream
                            .request(ClientRequest::Put {
                                key: key.to_string(),
                                value: value.to_string(),
                                request_id: *timestamp,
                            })
                            .await
                            .unwrap();
                    }
                }
            })
    });

    c.bench_with_input(BenchmarkId::new("Get", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let connection = connection.clone();
                async move {
                    let mut stream = connection.stream().await.unwrap();
                    for (key, _, timestamp) in kvs {
                        stream
                            .request(ClientRequest::Get {
                                key: key.to_string(),
                                request_id: *timestamp,
                            })
                            .await
                            .unwrap();
                    }
                }
            })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
