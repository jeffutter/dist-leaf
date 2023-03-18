use env_logger::Env;
use net::KVRequestType;
use s2n_quic::{client::Connect, Client};
use std::error::Error;
use std::net::ToSocketAddrs;
use std::thread;
use tokio::{
    io::{self, AsyncBufReadExt},
    runtime,
};

pub static CERT_PEM: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../cli/cert.pem"));

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // tracing_log::env_logger::init();
    env_logger::Builder::from_env(Env::default().default_filter_or(
        // "debug",
        "debug,netlink_proto=info,libp2p_ping=info,libp2p_swarm=info,libp2p_tcp=info,libp2p_mdns=info,libp2p_dns=info,yamux=info,multistream_select=info",
    ))
    .init();

    let args: Vec<String> = std::env::args().collect();

    if let Some(port) = args.get(1) {
        println!("port: {}", port);

        let client = Client::builder()
            .with_tls(CERT_PEM)?
            .with_io("0.0.0.0:0")?
            .start()?;

        let addr = format!("0.0.0.0:{}", port)
            .to_socket_addrs()?
            .next()
            .unwrap();
        println!("ADDR: {:?}", addr);
        // let connect = Connect::new(addr).with_server_name("localhost");
        // let connection = client.connect(connect).await?;
        // let addr: SocketAddr = "127.0.0.1:41229".parse()?;
        let connect = Connect::new(addr).with_server_name("localhost");
        let mut connection = client.connect(connect).await?;

        // ensure the connection doesn't time out with inactivity
        connection.keep_alive(true)?;

        // open a new stream and split the receiving and sending sides
        let stream = connection.open_bidirectional_stream().await?;
        let (mut receive_stream, mut send_stream) = stream.split();

        let _cli = tokio::spawn(async move {
            let mut stdin = io::BufReader::new(io::stdin()).lines();
            println!("Ready for Input");

            loop {
                let next_line = stdin
                    .next_line()
                    .await
                    .unwrap()
                    .expect("Stdin not to close");

                match handle_input_line(next_line) {
                    None => (),
                    Some(kv_request) => {
                        // let req = net::encode_get_request(key);
                        let req = net::encode_request(kv_request);
                        tokio::io::copy(&mut req.to_vec().as_slice(), &mut send_stream)
                            .await
                            .unwrap();

                        let mut buf = vec![];
                        tokio::io::copy(&mut receive_stream, &mut buf)
                            .await
                            .unwrap();
                        // let res = net::decode_get_response(buf);
                        let res = net::decode_request(&buf).unwrap();
                        println!("Result: {:?}", res);
                    }
                }
            }
        })
        .await
        .unwrap();
    } else {
        let core_ids = core_affinity::get_core_ids().unwrap();
        let handles = core_ids
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

        for handle in handles.into_iter() {
            handle.join().unwrap();
        }
    }

    Ok(())
}

fn handle_input_line(line: String) -> Option<KVRequestType> {
    let mut args = line.split(' ');

    let next = args.next().map(|x| x.to_uppercase());

    match next.as_deref() {
        Some("GET") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("Expected key");
                        return None;
                    }
                }
            };
            Some(net::KVRequestType::Get(key.to_string()))
        }
        Some("PUT") => {
            let key = {
                match args.next() {
                    Some(key) => key,
                    None => {
                        println!("Expected key");
                        return None;
                    }
                }
            };
            let value = {
                match args.next() {
                    Some(value) => value,
                    None => {
                        println!("Expected value");
                        return None;
                    }
                }
            };

            Some(KVRequestType::Put(key.to_string(), value.to_string()))
        }
        _ => {
            println!("expected GET, PUT");
            None
        }
    }
}
