use std::thread;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::{criterion_group, criterion_main, Criterion};
use env_logger::Env;
use itertools::Itertools;
use net::DistKVClient;
use tokio::runtime;

async fn put(kvs: &Vec<(&String, &String)>, client: DistKVClient) {
    for (key, value) in kvs {
        let request = net::KVRequestType::Put(key.to_string(), value.to_string());
        let req = client.send(request);
        req.await.unwrap();
    }
}

async fn get(kvs: &Vec<(&String, &String)>, client: DistKVClient) {
    for (key, _value) in kvs {
        let request = net::KVRequestType::Get(key.to_string());
        let req = client.send(request);
        req.await.unwrap();
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
    let mut dist_kv_server = rt.block_on(async { net::DistKVServer::new().unwrap() });
    let dist_kv_client = dist_kv_server.client();

    let core_ids = core_affinity::get_core_ids().unwrap();
    let _handles = core_ids
        .into_iter()
        .map(|id| {
            thread::spawn(move || {
                // Pin this thread to a single CPU core.
                let res = core_affinity::set_for_current(id);
                if res {
                    let rt = runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    log::info!("Starting Thread: #{:?}", id);
                    rt.block_on(async {
                        let mut dist_kv_server = net::DistKVServer::new().unwrap();
                        dist_kv_server.run().await.unwrap()
                    })
                }
            })
        })
        .collect::<Vec<_>>();

    rt.spawn(async move { dist_kv_server.run().await });

    log::info!("Sleeping for 5 Seconds for Nodes to Connect");
    thread::sleep(Duration::from_secs(5));

    c.bench_with_input(BenchmarkId::new("Put", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let dist_kv_client = dist_kv_client.clone();
                put(kvs, dist_kv_client)
            })
    });

    c.bench_with_input(BenchmarkId::new("Get", kvs.len()), &kvs, |b, kvs| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| {
                let dist_kv_client = dist_kv_client.clone();
                get(kvs, dist_kv_client)
            })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
