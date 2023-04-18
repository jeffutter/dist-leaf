use std::io::BufRead;
use std::net::SocketAddr;

use criterion::BenchmarkId;
use criterion::{criterion_group, criterion_main, Criterion};
use env_logger::Env;
use futures::Future;
use itertools::Itertools;
use quic_client::protocol::{ClientCommand, ClientCommandResponse};
use quic_client::ClientError;
use quic_client::{
    protocol::{ClientRequest, ClientResponse},
    DistKVClient,
};
use quic_transport::{MessageClient, QuicMessageClient};
use tokio::runtime;

async fn put(
    kvs: &Vec<(&String, &String)>,
    stream: impl Future<
        Output = Result<
            QuicMessageClient<ClientCommand, ClientRequest, ClientCommandResponse, ClientResponse>,
            ClientError,
        >,
    >,
) {
    let mut stream = stream.await.unwrap();
    for (key, value) in kvs {
        log::debug!("Putting: {} - {}", key, value);
        let request = ClientCommand::Put {
            key: key.to_string(),
            value: value.to_string(),
        };
        log::debug!("Putting Req - {:?}", request);
        stream.request(request).await.unwrap();
    }
}

async fn get(
    kvs: &Vec<(&String, &String)>,
    stream: impl Future<
        Output = Result<
            QuicMessageClient<ClientCommand, ClientRequest, ClientCommandResponse, ClientResponse>,
            ClientError,
        >,
    >,
) {
    let mut stream = stream.await.unwrap();
    for (key, _value) in kvs {
        log::debug!("Getting: {}", key);
        let request = ClientCommand::Get {
            key: key.to_string(),
        };
        stream.request(request).await.unwrap();
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
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();

    let dist_kv_connection = rt.block_on(async {
        let client = DistKVClient::new().unwrap();
        client.connect(addr).await.unwrap()
    });

    c.bench_with_input(BenchmarkId::new("Put", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let dist_kv_stream = dist_kv_connection.stream();
                put(kvs, dist_kv_stream)
            })
    });

    c.bench_with_input(BenchmarkId::new("Get", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let dist_kv_stream = dist_kv_connection.stream();
                get(kvs, dist_kv_stream)
            })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
