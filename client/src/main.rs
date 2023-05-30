mod mdns;
mod protocol;

use clap::{Parser, ValueEnum};
use env_logger::Env;
use futures::{channel::mpsc::channel, StreamExt};
use net::{quic::QuicClient, tcp::TcpClient, Client};
use protocol::{ClientRequest, ClientResponse};
use std::{
    error::Error,
    sync::{Arc, Mutex},
    thread,
};

pub mod client_capnp {
    include!(concat!(env!("OUT_DIR"), "/client_capnp.rs"));
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum Transport {
    TCP,
    Quic,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, value_enum)]
    transport: Transport,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // tracing_subscriber::fmt::init();
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let mdns = mdns::MDNS::new();
    let _mdns_jh = mdns.spawn().await;
    let mut mdns_rx = mdns.rx.clone();
    println!("Discovering Server via MDNS");
    mdns_rx.changed().await?;
    let server_cons = mdns.rx.borrow();
    let addr = server_cons.iter().next().unwrap();
    let hlc = Arc::new(Mutex::new(uhlc::HLC::default()));
    println!("Server Found: {:?}", addr);

    let client: Box<dyn Client<ClientRequest, ClientResponse>> = match args.transport {
        Transport::TCP => Box::new(TcpClient::new(*addr)?),
        Transport::Quic => Box::new(QuicClient::new(*addr)?),
    };
    println!("Client Built");
    let connection = client.connection().await?;
    println!("Connection Established");
    let mut stream = connection.stream().await?;
    println!("Stream Established");

    println!("Client Ready");

    let (mut tx, mut rx) = channel::<ClientRequest>(1);

    let cli = thread::spawn(move || {
        for line in std::io::stdin().lines() {
            let hlc = hlc.clone();
            match handle_input_line(hlc, line.unwrap()) {
                Some(req) => {
                    tx.try_send(req).unwrap();
                }
                None => (),
            }
        }
    });

    tokio::spawn(async move {
        loop {
            if let Some(req) = rx.next().await {
                let response: ClientResponse = stream.request(req).await.unwrap().into();
                println!("Response: {:?}", response);
            }
        }
    })
    .await
    .unwrap();

    cli.join().unwrap();

    Ok(())
}

fn handle_input_line(hlc: Arc<Mutex<uhlc::HLC>>, line: String) -> Option<ClientRequest> {
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
            Some(ClientRequest::Get {
                request_id: hlc.lock().unwrap().new_timestamp(),
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

            Some(ClientRequest::Put {
                request_id: hlc.lock().unwrap().new_timestamp(),
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
