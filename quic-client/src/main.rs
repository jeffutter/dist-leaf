mod protocol;

use env_logger::Env;
use protocol::{KVRequest, KVRequestType, KVResponse, KVResponseType};
use quic_client::DistKVClient;
use quic_transport::MessageClient;
use std::{error::Error, net::SocketAddr, thread};
use tokio::sync::mpsc::channel;

pub mod client_capnp {
    include!(concat!(env!("OUT_DIR"), "/client_capnp.rs"));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // tracing_subscriber::fmt::init();
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let args: Vec<String> = std::env::args().collect();
    let port = args.get(1).unwrap();

    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    let client: DistKVClient<KVRequest, KVRequestType, KVResponse, KVResponseType> =
        DistKVClient::new()?;
    let connection = client.connect(addr).await?;
    let mut stream = connection.stream().await?;

    println!("Client Ready");

    let (tx, mut rx) = channel::<KVRequest>(1);

    let cli = thread::spawn(move || {
        std::io::stdin()
            .lines()
            .for_each(|line| match handle_input_line(line.unwrap()) {
                Some(req) => {
                    tx.blocking_send(req).unwrap();
                }
                None => (),
            })
    });

    tokio::spawn(async move {
        loop {
            if let Some(req) = rx.recv().await {
                let response: KVResponse = stream.request(req).await.unwrap().into();
                println!("Response: {:?}", response);
            }
        }
    })
    .await
    .unwrap();

    cli.join().unwrap();

    Ok(())
}

fn handle_input_line(line: String) -> Option<KVRequest> {
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
            Some(KVRequest::Get {
                key: key.to_string(),
            })
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

            Some(KVRequest::Put {
                key: key.to_string(),
                value: value.to_string(),
            })
        }
        _ => {
            println!("expected GET, PUT");
            None
        }
    }
}
